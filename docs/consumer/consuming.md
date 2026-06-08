# Consumer

The consumer reads messages from Kafka and passes them to handlers. The entry point is `Bus::listener(string $workerName)`, which returns a listener with a blocking `listen()` method.

## Message Handlers

The package automatically resolves the handler argument type and passes the appropriate message representation. A handler is any callable: a class with `__invoke`, a closure, or a method.

### ConsumerMessageInterface — full message access

```php
use KafkaBus\Core\Interfaces\Consumers\Messages\ConsumerMessageInterface;

class ProductHandler
{
    public function __invoke(ConsumerMessageInterface $message): void
    {
        $payload = $message->payload();   // raw string from Kafka
        $headers = $message->headers();   // headers array
        $topic   = $message->topicName(); // topic name
    }
}
```

### string — payload only

```php
class ProductHandler
{
    public function __invoke(string $payload): void
    {
        // $payload — raw string from Kafka (e.g. JSON)
        $data = json_decode($payload, true);
    }
}
```

### array — payload as decoded JSON

```php
class ProductHandler
{
    public function __invoke(array $data): void
    {
        // $data — result of json_decode(payload, true)
        echo $data['id'];
    }
}
```

### RdKafka\Message — original rdkafka object

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
    }
}
```

## Custom Message Factory

The `#[MessageFactory]` attribute lets you control payload deserialization before it reaches the handler. This is the key integration point with [`kafka-bus/messages`](/docs/components/messages):

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
        echo $message->name;         // typed field
    }
}
```

## Starting the Listener

```php
$listener = $bus->listener('default');
$listener->listen(); // blocking loop
```

### Graceful Shutdown via Signals

```php
pcntl_async_signals(true);

$listener = $bus->listener('default');

pcntl_signal(SIGINT,  fn () => $listener->forceStop()); // Ctrl+C
pcntl_signal(SIGTERM, fn () => $listener->forceStop()); // kill

$listener->listen();
```
