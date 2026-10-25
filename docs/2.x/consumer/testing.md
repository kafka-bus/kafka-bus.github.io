---
title: Testing the consumer
description: Testing handlers, middleware, and workers without a Kafka broker using BusFaker and MessageFactory
---

# Testing the consumer

`kafka-bus/core` ships `BusFaker` and `MessageFactory`, which let you run the consumer side without a real Kafka broker. There are two levels of testing:

- **dispatch** — pass a message straight to `Bus::dispatch()` and check the handler; fast and doesn't need workers;
- **worker** — queue messages in the fake connection and run a real worker loop over them; checks topics, routing, and commits together.

::: tip Laravel
In a Laravel application, use `KafkaBus::fake()`, `KafkaBus::addMessage()`, `KafkaBus::dispatch()`, and `KafkaBus::listen()`. See [Testing in Laravel](/docs/2.x/laravel/testing).
:::

## Building test messages

`MessageFactory` builds `RdKafka\Message` objects. It's an immutable builder — every `with*` call returns a clone, so one factory instance can produce many variants.

```php
use KafkaBus\Core\Testing\Consumers\MessageFactory;

$message = MessageFactory::for()
    ->withTopicKey('products', $topicRegistry)
    ->withHeaders(['x-idempotency-key' => 'prod-42'])
    ->make('{"id":42,"name":"Laptop"}');
```

| Method | Default | Description |
|---|---|---|
| `withTopicKey(string $key, ?TopicRegistry $registry = null)` | `'test'` | Sets `topic_name`; with a registry, the key is resolved to the physical name |
| `withHeaders(array $headers)` | `[]` | Sets the message headers |
| `withKey(?string $key)` | `null` | Sets the partition key |
| `withPartition(int $partition)` | `0` | Sets the partition number |
| `withOffset(int $offset)` | `0` | Sets the offset |
| `make(string $payload)` | — | Returns the configured `RdKafka\Message` |
| `fromArray(array $attributes)` | — | JSON-encodes the array and calls `make()` |

A message built this way always has `err = RD_KAFKA_RESP_ERR_NO_ERROR`.

::: tip When to pass the registry
`BusFaker::addMessage()` and `BusFaker::dispatch()` resolve logical keys themselves, so `withTopicKey('products')` is enough for them. Pass the registry when you dispatch to a plain `Bus` or `Receiver` — routing works by physical topic name.
:::

## Testing a handler through dispatch

Build the receiver exactly as in production and pass it to `BusFaker::make()`:

```php
use KafkaBus\Core\Consumers\Messages\ConsumerMessage;
use KafkaBus\Core\Consumers\Messages\ConsumerMessageInterface;
use KafkaBus\Core\Receivers\Routing\ReceiverBuilder;
use KafkaBus\Core\Receivers\Routing\RouteInfo;
use KafkaBus\Core\Testing\Assertions\PHPUnitAssertionDriver;
use KafkaBus\Core\Testing\BusFaker;
use KafkaBus\Core\Testing\Consumers\MessageFactory;
use KafkaBus\Core\Topics\Topic;
use KafkaBus\Core\Topics\TopicRegistry;

$topicRegistry = (new TopicRegistry())
    ->add(new Topic('production.fact.products.1', 'products'));

$receiver = ReceiverBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', new ProductHandler()))
    ->build();

$bus = BusFaker::make($topicRegistry, new PHPUnitAssertionDriver(), receiver: $receiver);

$bus->dispatch(new ConsumerMessage(
    MessageFactory::for()
        ->withTopicKey('products')
        ->make('{"id":42,"name":"Laptop"}'),
));

$bus->assertDispatched('products');
$bus->assertDispatched(
    'products',
    static fn (ConsumerMessageInterface $message): bool => $message->payload() === '{"id":42,"name":"Laptop"}',
);
```

Dispatch alone never commits an offset — `assertNothingCommitted()` passes after it.

### Dispatch assertions

All methods take a topic key and resolve it via `TopicRegistry`:

| Method | Description |
|---|---|
| `assertDispatched($topicKey, ?$callback)` | At least one message was dispatched (and matches the callback) |
| `assertDispatchedTimes($topicKey, $times)` | Exactly N messages were dispatched |
| `assertNotDispatched($topicKey)` | Nothing was dispatched for the topic |
| `assertNothingDispatched()` | Nothing was dispatched at all |
| `getDispatched($topicKey)` | `list<ConsumerMessageInterface>` for the topic |
| `allDispatched()` | Every dispatched message |

A message is recorded as dispatched only after the handler succeeds.

## Testing the worker loop

`addMessage()` queues a message in the fake connection. A worker created with `WorkerRunnerFactory` reads the queue, dispatches each message, and commits it:

```php
use KafkaBus\Worker\Registry\MemoryWorkerRegistry;
use KafkaBus\Worker\Worker;
use KafkaBus\Worker\WorkerRunnerFactory;

$bus->addMessage(MessageFactory::for()->withTopicKey('products')->make('{"id":1}'));
$bus->addMessage(MessageFactory::for()->withTopicKey('products')->withOffset(1)->make('{"id":2}'));

$workerRegistry = MemoryWorkerRegistry::make()
    ->add(new Worker('products', [$topicRegistry->get('products')]));

(new WorkerRunnerFactory($workerRegistry))
    ->create($bus, 'products')
    ->run();

$bus->assertCommittedTimes('products', 2);
```

::: tip Why run() doesn't hang
The fake consumer serves messages from a pre-filled queue. Once it's empty, the loop exits cleanly, so `run()` returns on its own in tests.
:::

::: warning
The fake consumer copies the queue when it's created, in `WorkerRunnerFactory::create()`. Call `addMessage()` before `create()` — messages added later aren't read by that runner.
:::

### Commit assertions

| Method | Description |
|---|---|
| `assertCommitted($topicKey, ?$callback)` | At least one message was committed (and matches the callback) |
| `assertCommittedTimes($topicKey, $times)` | Exactly N messages were committed |
| `assertNothingCommitted()` | Nothing was committed |
| `getCommitted($topicKey)` | `list<ConsumerMessageInterface>` for the topic |

A message is committed only after `dispatch()` succeeds, so checking commits is a reliable sign that the whole chain worked.

## Testing handler errors

An exception from the handler leaves `dispatch()` — and the worker's `run()` — wrapped in a `MessageConsumerNotHandledException`:

```php
use KafkaBus\Core\Exceptions\Consumers\MessageConsumerNotHandledException;

$receiver = ReceiverBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', fn (string $payload) => throw new RuntimeException('boom')))
    ->build();

$bus = BusFaker::make($topicRegistry, new PHPUnitAssertionDriver(), receiver: $receiver);
$bus->addMessage(MessageFactory::for()->withTopicKey('products')->make('payload'));

try {
    (new WorkerRunnerFactory($workerRegistry))->create($bus, 'products')->run();
    $this->fail('Expected an exception');
} catch (MessageConsumerNotHandledException $exception) {
    $this->assertInstanceOf(RuntimeException::class, $exception->getPrevious());
}

$bus->assertNothingCommitted();
```

The reverse scenario is tested the same way: add a middleware that swallows the exception and check that the message was committed.

## Testing middleware

Attach middleware to the route exactly as in production — no extra setup is needed:

```php
$receiver = ReceiverBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', new ProductHandler(), [new ErrorHandlingMiddleware($logger)]))
    ->build();
```

## Test handlers

The `KafkaBus\Core\Testing\Messages` namespace contains two ready handlers for routes where the handler itself doesn't matter:

| Class | Argument | Behavior |
|---|---|---|
| `VoidConsumerHandlerFaker` | `ConsumerMessageInterface` | Does nothing |
| `ConsumerHandlerFaker` | `string` | Prints the payload |
