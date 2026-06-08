# Producer

## Creating a Message Class

Create a class in `app/Kafka/Messages/` and register it in the config:

```php
// app/Kafka/Messages/ProductMessage.php

namespace App\Kafka\Messages;

use KafkaBus\Core\Interfaces\Producers\Messages\ProducerMessageInterface;
use KafkaBus\Core\Interfaces\Producers\Messages\HasHeaders;
use KafkaBus\Core\Interfaces\Producers\Messages\HasKey;

final readonly class ProductMessage implements ProducerMessageInterface, HasHeaders, HasKey
{
    public function __construct(
        private int    $id,
        private string $name,
        private string $status,
    ) {}

    public function toPayload(): string
    {
        return json_encode([
            'id'     => $this->id,
            'name'   => $this->name,
            'status' => $this->status,
        ]);
    }

    // Provided when implementing the HasKey interface
    public function getKey(): ?string
    {
        // Kafka partition key — all events for the same product go to the same partition
        return (string) $this->id;
    }

    // Provided when implementing the HasHeaders interface
    public function getHeaders(): array
    {
        return ['event' => 'product.updated'];
    }
}
```

Register in the config:

```php
// config/kafka-bus.php
'producers' => [
    'routes' => [
        App\Kafka\Messages\ProductMessage::class => 'products',
    ],
],
```

## Publishing via Facade

```php
use Micromus\KafkaBusLaravel\Facades\KafkaBus;

KafkaBus::publish(new ProductMessage(
    id:     $product->id,
    name:   $product->name,
    status: $product->status,
));
```

## Publishing via Dependency Injection

Recommended for Action/Service classes — makes testing easier:

```php
use Micromus\KafkaBus\Interfaces\Bus\BusInterface;

class UpdateProductAction
{
    public function __construct(
        private readonly BusInterface $bus,
    ) {}

    public function execute(Product $product): void
    {
        $product->save();

        $this->bus->publish(new ProductMessage(
            id:     $product->id,
            name:   $product->name,
            status: $product->status,
        ));
    }
}
```

## Publishing via Another Connection

```php
KafkaBus::onConnection('analytics')->publish($message);

// Or via DI
$this->bus->onConnection('analytics')->publish($message);
```

## Integration with kafka-bus/messages

If the `kafka-bus/messages` package is installed, use `DomainMessage` for structured events:

```php
use Micromus\KafkaBusMessages\DomainMessage;
use Micromus\KafkaBusMessages\DomainEventEnum;

// In an event handler
$message = new ProductMessage(
    attributes: $product->toArray(),
    event: DomainEventEnum::Update,
    dirty: $product->getDirty(),
);

KafkaBus::publish($message);
```

The payload will look like:

```json
{
  "event": "update",
  "attributes": { "id": 42, "name": "Laptop Pro", "status": "active" },
  "dirty": ["name"]
}
```

Learn more about messages in the [Messages](/docs/components/messages) section.

## Adding an Idempotency Key

For guaranteed single processing on the consumer side, implement `HasIdempotency`:

```php
use Micromus\KafkaBusCommiter\Interfaces\HasIdempotency;

final readonly class ProductMessage implements ProducerMessageInterface, HasIdempotency
{
    public function getIdempotencyKey(): string
    {
        return "product-{$this->id}-v{$this->version}";
    }
}
```

And enable the middleware in the config:

```php
'producers' => [
    'middleware' => [
        KafkaBus\Commiter\Middleware\ProducerIdempotencyMiddleware::class,
    ],
],
```

Learn more in the [Commiter](/docs/components/commiter) section.
