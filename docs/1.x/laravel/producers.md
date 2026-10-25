# Producing Messages

## Creating a Message Class

Create a class in `app/Kafka/Messages/` and register it in the config:

```php
// app/Kafka/Messages/ProductMessage.php

namespace App\Kafka\Messages;

use KafkaBus\Core\Interfaces\Producers\Messages\HasHeaders;
use KafkaBus\Core\Interfaces\Producers\Messages\HasKey;
use KafkaBus\Core\Interfaces\Producers\Messages\ProducerMessageInterface;

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

    // Implemented because of the HasKey interface
    public function getKey(): ?string
    {
        // Kafka partition key — all events for one product go to the same partition
        return (string) $this->id;
    }

    // Implemented because of the HasHeaders interface
    public function getHeaders(): array
    {
        return ['event' => 'product.updated'];
    }
}
```

Registering it in the config:

```php
// config/kafka-bus.php
'producers' => [
    'routes' => [
        App\Kafka\Messages\ProductMessage::class => 'products',
    ],
],
```

More on the `HasKey`, `HasHeaders`, and `HasPartition` interfaces — in [Producing Messages](/docs/1.x/producer/producing#optional-message-interfaces).

## Publishing via the Facade

```php
use KafkaBus\Laravel\Facades\KafkaBus;

KafkaBus::publish(new ProductMessage(
    id:     $product->id,
    name:   $product->name,
    status: $product->status,
));
```

## Publishing via Dependency Injection

Recommended for Action/Service classes — makes testing simpler:

```php
use KafkaBus\Core\Interfaces\Bus\BusInterface;

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

`BusInterface` is registered as a singleton, so the type-hint resolves automatically. `KafkaBus::fake()` swaps the same container binding — tests using DI work exactly like tests using the facade.

## Batch Publishing

```php
use KafkaBus\Core\Bus\MessageBatch;
use KafkaBus\Laravel\Facades\KafkaBus;

$messages = $products
    ->map(fn (Product $p) => new ProductMessage($p->id, $p->name, $p->status))
    ->all();

KafkaBus::publishBatch(MessageBatch::fromArray($messages));
```

::: danger
Every message in a batch must be the **same class** — the route is resolved from the first element and applied to the whole batch. An empty array causes an error. More details — in [Batch Publishing](/docs/1.x/producer/producing#batch-publishing).
:::

## Publishing via a Different Connection

```php
KafkaBus::onConnection('analytics')->publish($message);

// Or via DI
$this->bus->onConnection('analytics')->publish($message);
```

Producer routes remain shared across all connections — only the target broker changes.

## Integrating with kafka-bus/messages

The `kafka-bus/messages` package is installed alongside `laravel-bridge`. Use `DomainMessage` for structured events:

```php
namespace App\Kafka\Messages;

use KafkaBus\Messages\Data\Casters\IntegerCaster;
use KafkaBus\Messages\DomainMessage;

/**
 * @property int    $id
 * @property string $name
 * @property string $status
 */
class ProductMessage extends DomainMessage
{
    public function getKey(): ?string
    {
        return (string) $this->id;
    }

    protected function definitionCasters(): array
    {
        return ['id' => new IntegerCaster()];
    }
}
```

Publishing from a model event handler:

```php
use KafkaBus\Laravel\Facades\KafkaBus;
use KafkaBus\Messages\DomainEventEnum;

$message = new ProductMessage(
    attributes: $product->toArray(),
    event: DomainEventEnum::Update,
    dirty: array_keys($product->getDirty()),
);

KafkaBus::publish($message);
```

The payload will look like this:

```json
{
  "event": "update",
  "attributes": { "id": 42, "name": "Laptop Pro", "status": "active" },
  "dirty": ["name"]
}
```

::: tip
Laravel's `Model::getDirty()` returns a `field => value` array, but `DomainMessage` expects a list of field names. Wrap it in `array_keys()`.
:::

More on messages — in [Messages](/docs/1.x/components/messages).

## Publishing from Model Events

A common way to avoid forgetting to dispatch an event is an observer:

```php
namespace App\Observers;

use App\Kafka\Messages\ProductMessage;
use KafkaBus\Laravel\Facades\KafkaBus;
use KafkaBus\Messages\DomainEventEnum;

class ProductObserver
{
    public function created(Product $product): void
    {
        KafkaBus::publish(ProductMessage::create($product->toArray(), DomainEventEnum::Create));
    }

    public function updated(Product $product): void
    {
        KafkaBus::publish(ProductMessage::create(
            $product->toArray(),
            DomainEventEnum::Update,
            array_keys($product->getDirty()),
        ));
    }

    public function deleted(Product $product): void
    {
        KafkaBus::publish(ProductMessage::create($product->toArray(), DomainEventEnum::Delete));
    }
}
```

::: warning Publishing and Database Transactions
`publish()` sends the message synchronously and waits for `flush()`. Inside a transaction that's dangerous: the message reaches Kafka even if the transaction later rolls back.

Publish after the commit — for example, via `DB::afterCommit()` or model `*ed` events outside a transaction. For strict guarantees, you need the Outbox pattern.
:::

## Adding an Idempotency Key

For guaranteed exactly-once processing on the consumer side, implement `HasIdempotency`:

```php
use KafkaBus\Commiter\Interfaces\HasIdempotency;
use KafkaBus\Core\Interfaces\Producers\Messages\ProducerMessageInterface;

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

The middleware adds an `x-idempotency-key` header. More details — in [Commiter](/docs/1.x/components/commiter).

## Inspecting Routes

```bash
php artisan kafka:route:list
```

This command shows every registered message class, its topic, and its middleware.
