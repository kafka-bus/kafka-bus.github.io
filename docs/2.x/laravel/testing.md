---
title: Testing in Laravel
description: Faking the Kafka Bus in Laravel tests — asserting publishes, dispatching messages, running workers, and checking commits
---

# Testing

The `KafkaBus` facade ships with a fake that works like `Event::fake()` or `Mail::fake()`. It replaces the transport only: routes, topics, handlers, and middleware stay real.

## Faking the bus

```php
use KafkaBus\Laravel\Facades\KafkaBus;

KafkaBus::fake();
```

The call creates a `FakeBus` on top of core's [`BusFaker`](/docs/2.x/producer/testing) and binds it in the container as `BusInterface`. The fake connection is bound as `ConnectionInterface`. Failed assertions are reported through PHPUnit, so they work in PHPUnit and Pest.

::: tip Works with dependency injection too
The `BusInterface` binding itself is swapped, so classes receiving `BusInterface` through their constructor also get the fake.

Call `KafkaBus::fake()` before the service under test is resolved from the container.
:::

## Asserting published messages

```php
it('publishes a message when a product is created', function () {
    KafkaBus::fake();

    app(CreateProductAction::class)->execute(id: 1, name: 'Laptop');

    KafkaBus::assertPublished(ProductMessage::class);
});
```

### Conditional assertion

The callback receives a `ProducerMessage` after the full pipeline — payload serialized, headers added:

```php
KafkaBus::assertPublished(
    ProductMessage::class,
    fn ($message) => str_contains($message->payload, '"id":1')
        && isset($message->headers['x-idempotency-key'])
);
```

Available `ProducerMessage` properties: `topic`, `payload`, `headers`, `partition`, `key` — all `readonly`.

### Other publish assertions

```php
KafkaBus::assertPublishedTimes(ProductMessage::class, 3);
KafkaBus::assertNotPublished(ProductMessage::class);
KafkaBus::assertNothingPublished();

$messages = KafkaBus::getPublished(ProductMessage::class); // list<ProducerMessage>
$all = KafkaBus::allPublished();
```

::: warning Messages are grouped by topic
`getPublished()` resolves the topic from the message class via the route table and returns every message on that topic. If several message classes route to the same topic, all of them are in the result.

For a class without a route, the method returns an empty array — so `assertNotPublished()` for it always passes. Check your routes with `kafka:publisher:route:list`.
:::

## Testing a handler

There are two ways to feed a message to your handler:

- `KafkaBus::dispatch()` — passes the message straight to the bus: routing, middleware, handler. No worker, no commit;
- `KafkaBus::addMessage()` + `KafkaBus::listen()` — runs a real worker loop over queued messages, including the commit.

### Building a test message

```php
use KafkaBus\Core\Testing\Consumers\MessageFactory;

$message = MessageFactory::for()
    ->withTopicKey('products')
    ->withHeaders(['x-idempotency-key' => 'prod-42'])
    ->make('{"id":42,"name":"Laptop"}');
```

`withTopicKey()` takes a logical key — the fake resolves it to the physical name with the topic prefix. Full method list — in [Testing the consumer](/docs/2.x/consumer/testing#building-test-messages).

### Dispatching a message

```php
use KafkaBus\Core\Consumers\Messages\ConsumerMessage;

it('stores a product when the message is received', function () {
    KafkaBus::fake();

    KafkaBus::dispatch(new ConsumerMessage(
        MessageFactory::for()
            ->withTopicKey('products')
            ->make('{"id":42,"name":"Laptop","status":"active"}')
    ));

    expect(Product::find(42)?->name)->toBe('Laptop');

    KafkaBus::assertDispatched('products');
});
```

### Running a worker

```php
it('reads and commits product messages', function () {
    KafkaBus::fake();

    $factory = MessageFactory::for()->withTopicKey('products');

    KafkaBus::addMessage($factory->withOffset(0)->make('{"id":1}'));
    KafkaBus::addMessage($factory->withOffset(1)->make('{"id":2}'));

    KafkaBus::listen('products');

    expect(Product::count())->toBe(2);

    KafkaBus::assertCommittedTimes('products', 2);
});
```

`listen()` takes a worker name — a key from `consumers.workers` or a route topic key — or a list of names. It returns on its own once the queued messages run out.

::: warning
Call `addMessage()` before `listen()`: each `listen()` takes a snapshot of the queue when it starts.
:::

::: tip
Use different offsets for messages on the same topic: `msgId()` is `"{partition}-{offset}-{topic}"`, and with `ConsumerCommiterMiddleware` messages with the same offset are treated as duplicates.
:::

### Several workers

```php
KafkaBus::addMessage(MessageFactory::for()->withTopicKey('products')->make('{"id":1}'));
KafkaBus::addMessage(MessageFactory::for()->withTopicKey('orders')->make('{"id":2}'));

KafkaBus::listen(['products', 'orders']);

KafkaBus::assertCommitted('products');
KafkaBus::assertCommitted('orders');
```

Topics of the listed workers are merged into one consumer stream — the same way `kafka:consume products orders` behaves. Every topic in the queue needs a route; a message without one stops `listen()` with a `MessageConsumerNotHandledException`.

### Handler errors

```php
use KafkaBus\Core\Exceptions\Consumers\MessageConsumerNotHandledException;

it('does not commit a message when the handler fails', function () {
    KafkaBus::fake();

    KafkaBus::addMessage(
        MessageFactory::for()->withTopicKey('products')->make('{"broken":true}')
    );

    expect(fn () => KafkaBus::listen('products'))
        ->toThrow(MessageConsumerNotHandledException::class);

    KafkaBus::assertNothingCommitted();
});
```

### Artisan commands under the fake

`kafka:consume` and `kafka:consume:routes` resolve the faked bus, so they read the queued messages and exit when the queue is empty:

```php
KafkaBus::fake();

KafkaBus::addMessage(MessageFactory::for()->withTopicKey('products')->make('{"id":1}'));

$this->artisan('kafka:consume:routes')->assertExitCode(0);

KafkaBus::assertCommitted('products');
```

## Dispatch and commit assertions

All these methods take a topic key, not a worker name, and resolve it via `TopicRegistry`. An unregistered key throws a `TopicCannotResolvedException`.

```php
KafkaBus::assertDispatched('products');
KafkaBus::assertDispatched('products', fn ($message) => $message->payload() === '{"id":1}');
KafkaBus::assertDispatchedTimes('products', 2);
KafkaBus::assertNotDispatched('orders');
KafkaBus::assertNothingDispatched();

KafkaBus::assertCommitted('products');
KafkaBus::assertCommitted('products', fn ($message) => $message->headers()['x-idempotency-key'] === 'prod-42');
KafkaBus::assertCommittedTimes('products', 2);
KafkaBus::assertNothingCommitted();
```

A message is recorded as dispatched after its handler succeeds, and as committed after the worker commits it. `dispatch()` alone never commits.

## Method reference

| Method | Purpose |
|---|---|
| `fake()` | Activates the fake |
| `assertPublished($class, ?$callback)` | The message was published |
| `assertPublishedTimes($class, $times)` | Published exactly N times |
| `assertNotPublished($class)` | The message wasn't published |
| `assertNothingPublished()` | Nothing was published |
| `getPublished($class)` | `list<ProducerMessage>` for the class |
| `allPublished()` | `list<ProducerMessage>` across every topic |
| `dispatch(ConsumerMessageInterface $message)` | Passes a message to the bus |
| `addMessage(RdKafka\Message $message)` | Queues a message for a worker |
| `listen(string\|array $workerName)` | Runs a worker over the queue |
| `assertDispatched($topicKey, ?$callback)` | A message was dispatched |
| `assertDispatchedTimes($topicKey, $times)` | Dispatched exactly N times |
| `assertNotDispatched($topicKey)` | Nothing was dispatched for the topic |
| `assertNothingDispatched()` | Nothing was dispatched |
| `getDispatched($topicKey)` | `list<ConsumerMessageInterface>` for the topic |
| `allDispatched()` | Every dispatched message |
| `assertCommitted($topicKey, ?$callback)` | A message was committed |
| `assertCommittedTimes($topicKey, $times)` | Committed exactly N times |
| `assertNothingCommitted()` | Nothing was committed |
| `getCommitted($topicKey)` | `list<ConsumerMessageInterface>` for the topic |

## Test environment

Configure a `null` connection so that tests never connect to a real broker:

```dotenv
# .env.testing
KAFKA_CONNECTION=testing
```

```php
// config/kafka-bus.php
'connections' => [
    'testing' => [
        'driver'  => 'null',
        'options' => [],
    ],
],
```

With `KafkaBus::fake()`, the connection doesn't matter — the fake replaces it. The `null` driver protects tests that don't call `fake()`: publishes are silently discarded.

::: warning
You can't read on a `null` connection: creating a consumer throws a `ConsumerException`. Always use `KafkaBus::fake()` for consumer tests.
:::
