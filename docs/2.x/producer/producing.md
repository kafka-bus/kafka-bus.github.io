---
title: Producing messages
description: Message classes, delivery contracts, and batch publishing through the Kafka Bus
---

# Producing messages

The producer is responsible for sending messages to Kafka. At the core level this is `Bus::publish()`, which accepts an object implementing `ProducerMessageInterface`.

## Main interface

The only required method is serialization to a string:

```php
namespace KafkaBus\Core\Producers\Messages;

interface ProducerMessageInterface
{
    public function toPayload(): string;
}
```

::: warning Don't confuse this with `ProducerMessage`
`KafkaBus\Core\Producers\Messages\ProducerMessage` is the **result** of the pipeline, not what you publish. It's assembled inside `PublisherPipelineHandler` and already carries the final `topic`, `payload`, `headers`, `partition`, and `key`. The class doesn't implement `ProducerMessageInterface`, so you can't pass it to `publish()`.
:::

## Custom message classes

Implement `ProducerMessageInterface` to encapsulate your serialization logic:

```php
use KafkaBus\Core\Producers\Messages\ProducerMessageInterface;
use KafkaBus\Core\Producers\Messages\HasKey;
use KafkaBus\Core\Producers\Messages\HasHeaders;

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
        return (string) $this->id; // all events for one product go to the same partition
    }

    public function getHeaders(): array
    {
        return ['event' => 'product.created'];
    }
}

$bus->publish(new ProductCreatedMessage(1, 'Laptop', 'Electronics'));
```

::: tip Ready-made base classes
If you'd rather not write `toPayload()` by hand, use `JsonMessage` or `DomainMessage` from the [messages component](/docs/2.x/components/messages) — they already implement `ProducerMessageInterface` on top of a typed `Payload`.
:::

## Optional message interfaces

Implement any combination of these interfaces on your message class to control how it's delivered. None of them are required — `PublisherPipelineHandler` checks for them via `instanceof` and falls back to defaults.

### HasKey

Returns the Kafka partition key for the message. Messages with the same key always land in the same partition, which guarantees ordering for that key.

```php
use KafkaBus\Core\Producers\Messages\HasKey;

public function getKey(): ?string
{
    return (string) $this->orderId; // all events for one order land in the same partition
}
```

Return `null` to let Kafka choose the partition itself (round-robin or random, depending on the broker version). Without the interface, the key is always `null`.

### HasHeaders

Attaches metadata headers to the message. Headers are merged with any added via `PublisherPipelineHandler::withHeader()`; on key collisions, headers passed through `withHeader()` from middleware take priority.

```php
use KafkaBus\Core\Producers\Messages\HasHeaders;

public function getHeaders(): array
{
    return [
        'event'   => 'order.created',
        'version' => '2',
        'source'  => 'order-service',
    ];
}
```

### HasPartition

Pins the message to a specific partition number. The value is floored at the `RD_KAFKA_PARTITION_UA` constant (`-1`) via `max()`, so returning a negative number is equivalent to not implementing the interface at all.

```php
use KafkaBus\Core\Producers\Messages\HasPartition;

public function getPartition(): int
{
    return 3; // always deliver to partition 3
}
```

::: tip
Prefer `HasKey` for guaranteed ordering. Use `HasPartition` only when you need explicit control over the partition — for example, when consuming and re-publishing to a mirror topic.
:::

### Putting it together

| What               | Source                                        | Value without the interface |
|---------------------|--------------------------------------------------|---------------------------------|
| `payload`          | `toPayload()`                                   | — (required)                    |
| `key`              | `HasKey::getKey()`                              | `null`                          |
| `headers`          | `HasHeaders::getHeaders()` + `withHeader()`     | `[]`                             |
| `partition`        | `HasPartition::getPartition()`                  | `RD_KAFKA_PARTITION_UA`         |
| `topic`            | producer route                                   | — (required)                    |

## Batch publishing

Use `Bus::publishBatch()` to send several messages in one call — they all go out through a single `PublisherStream` with a single `flush()` at the end:

```php
use KafkaBus\Core\Publishers\MessageBatch;

$messages = [];

foreach ($products as $product) {
    $messages[] = new ProductCreatedMessage($product->id, $product->name);
}

$bus->publishBatch(MessageBatch::fromArray($messages));
```

::: danger All messages in a batch must be the same class
`MessageBatch::fromArray()` resolves the route from the class of the **first** element and applies it to the whole batch. If you mix in a message of a different class, it will go to the wrong topic — with no error or warning (unless a static analyzer is set up).

`fromArray()` also accesses the **first** element directly: an empty array causes an error. Make sure the list isn't empty.
:::

The `MessageBatch` constructor can also be called directly if the class is known ahead of time — this is the only way to publish a batch from an `iterable`, such as a generator:

```php
$bus->publishBatch(new MessageBatch(ProductCreatedMessage::class, $generator));
```

`Bus::publish()` is simply a special case: it wraps a single message in `MessageBatch::fromArray([$message])`.

## Publishing to another cluster

A `Bus` is bound to one connection. To publish to a second cluster, build a separate `Bus` on that connection — the publisher routes can be shared:

```php
$analyticsBus = new Bus($connectionRegistry->connection('analytics'), $publisherFactory);

$analyticsBus->publish($message);
```

::: info Changed in 2.0
`Bus::onConnection()` was removed. See [Upgrading from 1.x](/docs/2.x/upgrade#multiple-connections).
:::

## What's next

- [Configure Producer](/docs/2.x/producer/configure) — routes, flush, options
- [Pipeline](/docs/2.x/producer/pipeline) — outgoing message middleware
- [Testing](/docs/2.x/producer/testing) — verifying publishes without a broker
