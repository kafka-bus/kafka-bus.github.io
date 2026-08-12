# Testing

`kafka-bus/core` ships fakes and helpers that let you run the full consumer pipeline without a real Kafka broker.

::: tip Laravel
In a Laravel application there's no wiring to do — use `KafkaBus::fake()`, `KafkaBus::addMessage()`, and `KafkaBus::listen()`. See [Testing in Laravel](/docs/laravel/testing).
:::

## MessageFactory

`MessageFactory` builds `RdKafka\Message` objects for use in tests. It follows the immutable builder pattern — every `with*` call returns a clone, so a single factory instance can produce many variations.

```php
use KafkaBus\Core\Testing\Consumers\MessageFactory;

$message = MessageFactory::for()
    ->withTopicKey('products')
    ->withHeaders(['x-idempotency-key' => 'prod-42'])
    ->make('{"id":42,"name":"Laptop"}');
```

### Available Methods

| Method                           | Default    | Description                                |
|-------------------------------------|--------------|------------------------------------------------|
| `withTopicKey(string $key)`     | `'test'`    | Sets the message's `topic_name`                |
| `withHeaders(array $headers)`   | `[]`        | Sets the message headers                        |
| `withKey(?string $key)`         | `null`      | Sets the partition key                          |
| `withPartition(int $partition)` | `0`         | Sets the partition number                        |
| `withOffset(int $offset)`       | `0`         | Sets the offset                                  |
| `make(string $payload)`         | —           | Returns the configured `RdKafka\Message`         |
| `fromArray(array $attributes)`  | —           | JSON-encodes the array and calls `make()`        |

A message built this way always has `err = RD_KAFKA_RESP_ERR_NO_ERROR`, so it passes validation in `Consumer::getMessage()` as a valid message.

::: tip
`withTopicKey()` is written into the `topic_name` field, but `ConnectionFaker::addMessage()` runs it through `TopicRegistry::tryGetTopicName()`. So you can pass either a logical key (`products`) or a ready-made physical name — both work.
:::

### Building a Message from an Array

```php
$message = MessageFactory::for()
    ->withTopicKey('products')
    ->fromArray(['id' => 42, 'name' => 'Laptop']);
```

`fromArray()` uses `json_encode()` with the `JSON_THROW_ON_ERROR` flag.

## A Complete Consumer Test

The example below is a framework-agnostic integration test. `ConnectionFaker` and `ConnectionRegistryFaker` replace the real broker — the whole pipeline (routing, handler, commit) runs entirely in memory.

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

// 3. Configure routes and handlers
$consumerRoutes = (new ConsumerRoutes())
    ->add(new Route(
        topic: $topicRegistry->get('products'),
        handler: new YourHandler(),
    ));

$workerRegistry = (new MemoryWorkerRegistry())
    ->add(new Worker('default-listener', $consumerRoutes));

// 4. Assemble Bus with fake connections
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

::: tip Why listen() doesn't hang
`ConsumerFaker` serves messages from a pre-filled array. Once messages run out, it throws an internal `KafkaMessagesEndedException`, which `ConsumerStream` catches and uses to exit the loop cleanly. So `listen()` doesn't block in tests — there's no need to stop it manually.
:::

::: warning
Routes here are created directly via `new Route(...)`. Note that the consumer's `Route` takes a ready-made `Topic` object rather than a key, and defaults to `NativeMessageFactory` — auto-detection by handler argument type **doesn't work** in this case. To get production-like behavior, build routes via `ConsumerRoutesBuilder` instead:

```php
use KafkaBus\Core\Consumers\Router\ConsumerRoutesBuilder;
use KafkaBus\Core\Consumers\Router\RouteInfo;

$consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', new YourHandler()))
    ->build();
```
:::

### Asserting Committed Messages

After `listen()` returns, every successfully processed message is recorded in `$connectionFaker->committedMessages`, keyed by the full topic name:

```php
$committed = $connectionFaker->committedMessages['production.fact.products.1'];

assert(count($committed) === 1);

$message = $committed[0]; // ConsumerMessageInterface

assert($message->payload() === 'test-message');
assert($message->headers() === ['foo' => 'bar']);
assert($message->topicName() === 'production.fact.products.1');

$original = $message->original(); // RdKafka\Message
assert($original->offset === 0);
```

A message only appears in `committedMessages` after the handler succeeds. If the handler throws, there's no entry. This makes checking commits a reliable indicator that the whole pipeline ran correctly.

## Multiple Messages

`addMessage()` can be called repeatedly — messages are processed in the order they were added:

```php
$factory = MessageFactory::for()->withTopicKey('products');

$connectionFaker->addMessage($factory->withOffset(0)->fromArray(['id' => 1]));
$connectionFaker->addMessage($factory->withOffset(1)->fromArray(['id' => 2]));
$connectionFaker->addMessage($factory->withOffset(2)->fromArray(['id' => 3]));

$bus->listener('default-listener')->listen();

assert(count($connectionFaker->committedMessages['production.fact.products.1']) === 3);
```

The builder's immutability is especially handy here: the base `$factory` never changes, and every `withOffset()` call produces a new variant.

## Testing Handler Errors

To test failure behavior, throw an exception from the handler — it surfaces from `listen()` wrapped in a `MessageConsumerNotHandledException`:

```php
use KafkaBus\Core\Exceptions\Consumers\MessageConsumerNotHandledException;

$consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', fn (string $payload) => throw new RuntimeException('boom')))
    ->build();

try {
    $bus->listener('default-listener')->listen();
    assert(false, 'Expected an exception');
}
catch (MessageConsumerNotHandledException $e) {
    assert($e->getPrevious() instanceof RuntimeException);
}

// The message wasn't committed
assert($connectionFaker->committedMessages === []);
```

The reverse scenario is tested the same way: add middleware that swallows the exception and confirm the message still ends up in `committedMessages`.

## Testing Middleware

Middleware is attached to a worker or route exactly the same way as in production — no extra setup needed:

```php
$workerRegistry = (new MemoryWorkerRegistry())
    ->add(new Worker(
        name:    'default-listener',
        routes:  $consumerRoutes,
        options: new Options(middleware: [new ErrorHandlingMiddleware($logger)]),
    ));
```

Since `committedMessages` only fills up on a successful run, the commit itself confirms the middleware chain completed end to end.
