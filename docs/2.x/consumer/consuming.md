---
title: Consuming messages
description: Message handlers, automatic argument typing, and message factories for incoming Kafka messages
---

# Consuming messages

Receiving a message in Kafka Bus 2.x has two independent parts. A worker reads raw messages from Kafka and calls `Bus::dispatch()`. The bus finds the route for the message's topic and calls your handler. This page is about the second part — how to write a handler and how the message is converted into the argument the handler expects.

```php
$bus->dispatch($consumerMessage); // ReceiverRouter → middleware → handler
```

To configure routes, see [Consumer routes](/docs/2.x/consumer/configure). To run the polling loop, see [Workers](/docs/2.x/consumer/workers).

## Message handlers

The package automatically detects the handler argument's type and passes the matching message representation. A handler is any callable: a class with `__invoke`, or a closure.

::: warning
Auto-detection works via reflection and only supports two kinds of callables: a closure (`Closure`) and an object with an `__invoke()` method. For string callables like `'Class::method'` or an array like `[$obj, 'method']`, the type can't be detected — the default factory (`NativeMessageFactory`) is used instead.
:::

### Full message access

```php
use KafkaBus\Core\Consumers\Messages\ConsumerMessageInterface;

class ProductHandler
{
    public function __invoke(ConsumerMessageInterface $message): void
    {
        $payload = $message->payload();   // raw string from Kafka
        $headers = $message->headers();   // array of headers
        $topic   = $message->topicName(); // physical topic name
        $key     = $message->key();       // partition key or null
        $id      = $message->msgId();     // "{partition}-{offset}-{topic_name}"
        $raw     = $message->original();  // RdKafka\Message
    }
}
```

This same representation is used by default when the argument type can't be detected.

::: tip
`key()` returns `null` when the key is empty: rdkafka returns `''` instead of `null`, and `ConsumerMessage` normalizes that value.
:::

### Payload only

```php
class ProductHandler
{
    public function __invoke(string $payload): void
    {
        // $payload — the raw string from Kafka (e.g. JSON)
        $data = json_decode($payload, true);
    }
}
```

### Payload as decoded JSON

```php
class ProductHandler
{
    public function __invoke(array $data): void
    {
        // $data — the result of json_decode(payload, true)
        echo $data['id'];
    }
}
```

### Original rdkafka object

When you need access to low-level metadata: offset, partition, timestamp:

```php
use RdKafka\Message;

class ProductHandler
{
    public function __invoke(Message $message): void
    {
        echo $message->key;       // Kafka partition key
        echo $message->offset;    // current offset
        echo $message->partition; // partition number
        echo $message->timestamp; // message timestamp
    }
}
```

## Custom message factory

The `#[MessageFactory]` attribute lets you control payload deserialization before it reaches the handler. This is the key integration point with the [messages component](/docs/2.x/components/messages):

```php
use KafkaBus\Core\Receivers\Attributes\MessageFactory;
use KafkaBus\Core\Receivers\Messages\JsonMessageFactory;

class ProductHandler
{
    #[MessageFactory(new JsonMessageFactory())]
    public function __invoke(array $data): void
    {
        // $data — guaranteed to be decoded JSON
    }
}
```

For typed domain messages from the `kafka-bus/messages` package:

```php
use KafkaBus\Messages\Factories\DomainMessageFactory;

class ProductHandler
{
    #[MessageFactory(new DomainMessageFactory(ProductMessage::class))]
    public function __invoke(ProductMessage $message): void
    {
        echo $message->getEvent()->value; // 'create' | 'update' | 'delete'
        echo $message->name;              // typed field
    }
}
```

::: tip The attribute goes on `__invoke`
`MessageFactoryExtractor` looks for the attribute on the `__invoke()` method itself (or on the closure), not on the class. The attribute is declared as `Attribute::TARGET_METHOD`.
:::

### Factory resolution order

`MessageFactoryExtractor` picks a factory using the first rule that matches:

1. **The `#[MessageFactory]` attribute** on `__invoke()` — used if present;
2. **The first argument's type**:
   - `string` → `StringMessageFactory`
   - `array` → `JsonMessageFactory`
   - `RdKafka\Message` → `OriginalMessageFactory`
3. **The default factory** passed to `ReceiverBuilder::make()` / `ReceiverRoutesBuilder::make()` — usually `NativeMessageFactory`, which returns the `ConsumerMessageInterface` itself.

### Built-in factories

| Factory                  | Returns                    | When it applies                        |
|----------------------------|-------------------------------|--------------------------------------------|
| `NativeMessageFactory`  | `ConsumerMessageInterface` | Default                                     |
| `StringMessageFactory`  | `string`                   | Argument of type `string`                    |
| `JsonMessageFactory`    | `array`                    | Argument of type `array`                     |
| `OriginalMessageFactory`| `RdKafka\Message`          | Argument of type `RdKafka\Message`           |

### Custom factory

Implement `MessageFactoryInterface` — its single method receives the message and returns whatever you need:

```php
use KafkaBus\Core\Consumers\Messages\ConsumerMessageInterface;
use KafkaBus\Core\Receivers\Messages\MessageFactoryInterface;

final class ProtobufMessageFactory implements MessageFactoryInterface
{
    public function fromKafka(ConsumerMessageInterface $message): ProductProto
    {
        $proto = new ProductProto();
        $proto->mergeFromString($message->payload());

        return $proto;
    }
}
```

```php
class ProductHandler
{
    #[MessageFactory(new ProtobufMessageFactory())]
    public function __invoke(ProductProto $product): void { /* ... */ }
}
```

## Dispatching a message

`Bus::dispatch()` accepts any `ConsumerMessageInterface`. In production a worker calls it for every message it reads; you can also call it yourself — for example, to replay a message from a dead-letter table:

```php
use KafkaBus\Core\Consumers\Messages\ConsumerMessage;

$bus->dispatch(new ConsumerMessage($rdkafkaMessage));
```

What `dispatch()` does:

1. `ReceiverRouter` finds the route by `topicName()` of the message;
2. the route's message factory converts the message into the handler argument;
3. the route's middleware run, then the handler.

Any exception thrown along the way — no route, a factory error, a handler error — is wrapped in a `MessageConsumerNotHandledException`. The original is available via `getPrevious()`:

```php
use KafkaBus\Core\Exceptions\Consumers\MessageConsumerNotHandledException;

try {
    $bus->dispatch($message);
} catch (MessageConsumerNotHandledException $exception) {
    $exception->consumerMessage; // the message that failed
    $exception->getPrevious();   // the original exception
}
```

`dispatch()` doesn't commit offsets — that's the worker's job, done only after `dispatch()` returns. See [Committing offsets](/docs/2.x/consumer/workers#committing-offsets).

## What's next

- [Consumer routes](/docs/2.x/consumer/configure) — binding topics to handlers
- [Workers](/docs/2.x/consumer/workers) — reading Kafka and graceful shutdown
- [Pipeline](/docs/2.x/consumer/pipeline) — middleware for incoming messages
