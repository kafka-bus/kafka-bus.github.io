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

## Optional Message Interfaces

Implement any combination of these interfaces on your message class to control how it is delivered.

### HasKey

```php
use KafkaBus\Core\Interfaces\Producers\Messages\HasKey;
```

Returns the Kafka partition key for the message. Messages that share the same key are always routed to the same partition, which guarantees ordering for that key.

```php
public function getKey(): ?string
{
    return (string) $this->orderId; // all events for the same order land in the same partition
}
```

Return `null` to let Kafka choose a partition freely (round-robin or random, depending on the broker version).

---

### HasHeaders

```php
use KafkaBus\Core\Interfaces\Producers\Messages\HasHeaders;
```

Attaches metadata headers to the message. Headers are merged with any headers added via `ProducerPipelineHandler::withHeader()`, with the per-call headers taking precedence on duplicate keys.

```php
public function getHeaders(): array
{
    return [
        'event'   => 'order.created',
        'version' => '2',
        'source'  => 'order-service',
    ];
}
```

---

### HasPartition

```php
use KafkaBus\Core\Interfaces\Producers\Messages\HasPartition;
```

Pins the message to a specific partition number. The value is clamped to `RD_KAFKA_PARTITION_UA` (`-1`) as a minimum, so returning a negative number is equivalent to not implementing the interface at all.

```php
public function getPartition(): int
{
    return 3; // always deliver to partition 3
}
```

> Prefer `HasKey` for ordering guarantees. Use `HasPartition` only when you need explicit partition control, for example when consuming and re-publishing to a mirrored topic.

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
