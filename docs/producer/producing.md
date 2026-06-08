# Producer

The producer is responsible for sending messages to Kafka. At the core level this is `Bus::publish()`, which receives an object implementing `ProducerMessageInterface`.

## ProducerMessage

Base class for outgoing messages:

```php
use KafkaBus\Core\Producers\Messages\ProducerMessage;

$bus->publish(new ProducerMessage(
    payload: json_encode(['id' => 1, 'name' => 'Laptop']),
    headers: ['source' => 'catalog-service', 'version' => '1'],
    key:     '1', // Kafka partition key (optional)
));
```

## Custom Message Classes

Implement `ProducerMessageInterface` to encapsulate serialization logic:

```php
use KafkaBus\Core\Interfaces\Producers\Messages\ProducerMessageInterface;
use KafkaBus\Core\Interfaces\Producers\Messages\HasKey;
use KafkaBus\Core\Interfaces\Producers\Messages\HasHeaders;

final readonly class ProductCreatedMessage implements ProducerMessageInterface, HasKey, HasHeaders
{
    public function __construct(
        private int    $id,
        private string $name,
        private string $category,
    ) {}

    public function toPayload(): string
    {
        return json_encode([
            'id'       => $this->id,
            'name'     => $this->name,
            'category' => $this->category,
        ]);
    }

    public function getKey(): ?string
    {
        return (string) $this->id; // Ensures all events for the same product go to the same partition
    }

    public function getHeaders(): array
    {
        return ['event' => 'product.created'];
    }
}

$bus->publish(new ProductCreatedMessage(1, 'Laptop', 'Electronics'));
```

## Batch Publishing

Use `Bus::publishBatch()` to send multiple messages at once:

```php
namespace KafkaBus\Core\Bus\MessageBatch;

$messages = [];

foreach ($products as $product) {
    $messages[] = new ProductCreatedMessage($product->id, $product->name);
}

$bus->publishBatch(MessageBatch::fromArray($messages));
```

## Switching Connections

```php
$bus->onConnection('analytics')->publish($message);
```
