# Consuming Messages

The consumer reads messages from Kafka and dispatches them to handlers. The entry point is `Bus::listener(string|array $workerName)`, which returns a listener with a blocking `listen()` method.

## Message Handlers

The package automatically detects the handler argument's type and passes the matching message representation. A handler is any callable: a class with `__invoke`, or a closure.

::: warning
Auto-detection works via reflection and only supports two kinds of callables: a closure (`Closure`) and an object with an `__invoke()` method. For string callables like `'Class::method'` or an array like `[$obj, 'method']`, the type can't be detected — the default factory (`NativeMessageFactory`) is used instead.
:::

### Full Message Access

```php
use KafkaBus\Core\Interfaces\Consumers\Messages\ConsumerMessageInterface;

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

### Payload Only

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

### Payload as Decoded JSON

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

### Original rdkafka Object

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

## Custom Message Factory

The `#[MessageFactory]` attribute lets you control payload deserialization before it reaches the handler. This is the key integration point with [`kafka-bus/messages`](/docs/1.x/components/messages):

```php
use KafkaBus\Core\Consumers\Attributes\MessageFactory;
use KafkaBus\Core\Consumers\Messages\JsonMessageFactory;

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

### Factory Resolution Order

`MessageFactoryExtractor` picks a factory using the first rule that matches:

1. **The `#[MessageFactory]` attribute** on `__invoke()` — used if present;
2. **The first argument's type**:
   - `string` → `StringMessageFactory`
   - `array` → `JsonMessageFactory`
   - `RdKafka\Message` → `OriginalMessageFactory`
3. **The default factory** passed to `ConsumerRoutesBuilder::make()` — usually `NativeMessageFactory`, which returns the `ConsumerMessageInterface` itself.

### Built-in Factories

| Factory                  | Returns                    | When it applies                        |
|----------------------------|-------------------------------|--------------------------------------------|
| `NativeMessageFactory`  | `ConsumerMessageInterface` | Default                                     |
| `StringMessageFactory`  | `string`                   | Argument of type `string`                    |
| `JsonMessageFactory`    | `array`                    | Argument of type `array`                     |
| `OriginalMessageFactory`| `RdKafka\Message`          | Argument of type `RdKafka\Message`           |

### Custom Factory

Implement `MessageFactoryInterface` — its single method receives the message and returns whatever you need:

```php
use KafkaBus\Core\Interfaces\Consumers\Messages\ConsumerMessageInterface;
use KafkaBus\Core\Interfaces\Consumers\Messages\MessageFactoryInterface;

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

## Starting the Listener

```php
$listener = $bus->listener('default');
$listener->listen(); // blocking loop
```

`listen()` doesn't return control until `forceStop()` is called or a non-ignorable error occurs. The loop works like this:

1. `Consumer::getMessage()` — waits for a message for up to `consumerTimeout` ms;
2. `MessageHandler::handle()` — worker pipeline, routing, handler;
3. `Consumer::commit()` — confirms the offset.

The commit happens **only after the handler succeeds**: if the handler throws, the exception is wrapped in a `MessageConsumerNotHandledException` and propagates outward, breaking the loop.

::: tip How to avoid crashing the worker on errors
To keep one "bad" message from stopping processing, catch the exception in worker middleware — see [Consumer Pipeline](/docs/1.x/consumer/pipeline#middleware-with-error-handling).
:::

### Listening for Multiple Workers

```php
$bus->listener(['products', 'orders'])->listen();
```

Workers are merged into a single process via `WorkerMerger`, with each worker keeping its own middleware set. Connection settings are taken from the first worker in the list — more details in [Architecture](/docs/1.x/architecture#multiple-workers-in-one-process).

### Graceful Shutdown via Signals

```php
pcntl_async_signals(true);

$listener = $bus->listener('default');

pcntl_signal(SIGINT,  fn () => $listener->forceStop()); // Ctrl+C
pcntl_signal(SIGTERM, fn () => $listener->forceStop()); // kill

$listener->listen();
```

`forceStop()` sets a flag that's checked at the end of each loop iteration. So the shutdown happens only after the current message has been processed and committed — the process won't stop halfway through.

::: tip
In Laravel this is already handled for you: the `kafka:consume` command implements `SignalableCommandInterface` and handles `SIGTERM`, `SIGINT`, and `SIGQUIT`.
:::
