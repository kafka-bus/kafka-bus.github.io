# Testing

`kafka-bus/core` ships with fakes and helpers that let you run the full consumer pipeline without a real Kafka broker.

## MessageFactory

`MessageFactory` builds `RdKafka\Message` objects for use in tests. It follows the immutable builder pattern — every `with*` call returns a clone, so one factory instance can produce many variations.

```php
use KafkaBus\Core\Testing\Consumers\MessageFactory;

$message = MessageFactory::for()
    ->withTopicKey('products')
    ->withHeaders(['x-idempotency-key' => 'prod-42'])
    ->make('{"id":42,"name":"Laptop"}');
```

### Available Methods

| Method | Default | Description |
|---|---|---|
| `withTopicKey(string $key)` | `'test'` | Sets `topic_name` on the message |
| `withHeaders(array $headers)` | `[]` | Sets message headers |
| `withKey(?string $key)` | `null` | Sets the partition key |
| `withPartition(int $partition)` | `0` | Sets the partition number |
| `withOffset(int $offset)` | `0` | Sets the offset |
| `make(string $payload)` | — | Returns a configured `RdKafka\Message` |
| `fromArray(array $attributes)` | — | JSON-encodes the array and calls `make()` |

### Building from an Array

```php
$message = MessageFactory::for()
    ->withTopicKey('products')
    ->fromArray(['id' => 42, 'name' => 'Laptop']);
```

## Full Consumer Test

The example below is a framework-agnostic integration test. `ConnectionFaker` and `ConnectionRegistryFaker` replace the real broker — the complete pipeline (routing, handler, commit) runs entirely in memory.

```php
use KafkaBus\Core\Bus;
use KafkaBus\Core\Bus\Listeners\ListenerFactory;
use KafkaBus\Core\Bus\Listeners\Workers\MemoryWorkerRegistry;
use KafkaBus\Core\Bus\Listeners\Workers\Worker;
use KafkaBus\Core\Bus\Publishers\PublisherFactory;
use KafkaBus\Core\Bus\ThreadFactory;
use KafkaBus\Core\Bus\ThreadRegistry;
use KafkaBus\Core\Consumers\ConsumerStreamFactory;
use KafkaBus\Core\Consumers\Handlers\MessageHandlerFactory;
use KafkaBus\Core\Consumers\Router\ConsumerRoutes;
use KafkaBus\Core\Consumers\Router\Route;
use KafkaBus\Core\Producers\ProducerStreamFactory;
use KafkaBus\Core\Testing\Connections\ConnectionFaker;
use KafkaBus\Core\Testing\Connections\ConnectionRegistryFaker;
use KafkaBus\Core\Testing\Consumers\MessageFactory;
use KafkaBus\Core\Topics\Topic;
use KafkaBus\Core\Topics\TopicRegistry;

// 1. Register topics
$topicRegistry = (new TopicRegistry())
    ->add(new Topic('production.fact.products.1', 'products'));

$connectionFaker = new ConnectionFaker($topicRegistry);

// 2. Add a test message to the fake connection
$connectionFaker->addMessage(
    MessageFactory::for()
        ->withHeaders(['foo' => 'bar'])
        ->withTopicKey('products')
        ->make('test-message')
);

// 3. Wire up routes and handlers
$consumerRoutes = (new ConsumerRoutes())
    ->add(new Route(
        topic: $topicRegistry->get('products'),
        handler: new YourHandler(),
    ));

$workerRegistry = (new MemoryWorkerRegistry())
    ->add(new Worker('default-listener', $consumerRoutes));

// 4. Build the Bus with fake connections
$bus = new Bus(
    new ThreadRegistry(
        new ConnectionRegistryFaker($connectionFaker),
        new ThreadFactory(
            new ListenerFactory(
                new ConsumerStreamFactory(new MessageHandlerFactory()),
                $workerRegistry
            ),
            new PublisherFactory(new ProducerStreamFactory())
        )
    ),
    'default'
);

// 5. Run the full pipeline
$bus->listener('default-listener')->listen();
```

### Asserting Committed Messages

After `listen()` returns, every committed message is recorded in `$connectionFaker->committedMessages`, keyed by the full topic name:

```php
$committed = $connectionFaker->committedMessages['production.fact.products.1'];

assert(count($committed) === 1);

$original = $committed[0]->original(); // RdKafka\Message

assert($original->payload === 'test-message');
assert($original->headers === ['foo' => 'bar']);
```

A message appears in `committedMessages` only after a successful handler run. If the handler throws, the entry is absent — making commit assertions a reliable signal that the full pipeline executed correctly.