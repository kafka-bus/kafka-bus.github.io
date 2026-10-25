# Testing

The `KafkaBus` facade ships with a full-featured fake, working just like `Event::fake()` or `Mail::fake()`.

## Faking the Bus in Tests

```php
use KafkaBus\Laravel\Facades\KafkaBus;

KafkaBus::fake();
```

This call swaps the container's `ConnectionRegistryInterface` for a `ConnectionRegistryFaker` and rebuilds `BusInterface`, then registers the resulting `FakeBus` as a singleton.

::: tip Works with DI too
It's the `BusInterface` binding itself that gets swapped, so classes receiving `BusInterface` through their constructor also get the fake — no need to rewrite code around the facade.

Just make sure to call `KafkaBus::fake()` **before** the service under test is resolved from the container.
:::

No real broker is involved: routes, topics, the pipeline, and middleware all stay real — only the transport is swapped out.

## Asserting Published Messages

### Basic Assertion

```php
it('publishes a message when a product is created', function () {
    KafkaBus::fake();

    app(CreateProductAction::class)->execute(id: 1, name: 'Laptop');

    KafkaBus::assertPublished(ProductMessage::class);
});
```

### Conditional Assertion

The callback receives a `ProducerMessage` after it's passed through the full pipeline (including payload serialization and header assignment):

```php
KafkaBus::assertPublished(
    ProductMessage::class,
    fn ($msg) => str_contains($msg->payload, '"id":1')
              && isset($msg->headers['x-idempotency-key'])
);
```

Available `ProducerMessage` properties: `topic`, `payload`, `headers`, `partition`, `key` — all `readonly`.

### Asserting a Count

```php
KafkaBus::assertPublishedTimes(ProductMessage::class, 3);
```

### Asserting Nothing Was Published

```php
KafkaBus::assertNotPublished(ProductMessage::class);
KafkaBus::assertNothingPublished();
```

### Retrieving Published Messages

```php
// All messages of the given type (post-middleware, with payload and headers)
$messages = KafkaBus::getPublished(ProductMessage::class);

// All published messages across every topic
$all = KafkaBus::allPublished();

expect($messages[0]->payload)->toContain('"id":1');
expect($messages[0]->headers)->toHaveKey('x-idempotency-key');
```

::: warning Messages Are Grouped by Topic
`getPublished()` resolves the topic from the message class via the route table, then returns **every** message on that topic. If several message classes route to the same topic, they'll all show up in the result.

For a class with no registered route, the method returns an empty array — so `assertNotPublished()` for it always passes, even if publishing it would have been impossible. Check your routes with `kafka:route:list`.
:::

## Testing the Consumer

`addMessage()` adds an `RdKafka\Message` to the fake connection, and `listen()` runs the full consumer pipeline — middleware, routing, handler, commit — with no real broker.

### Building a Test Message

```php
use KafkaBus\Core\Testing\Consumers\MessageFactory;

$message = MessageFactory::for()
    ->withTopicKey('products')
    ->withHeaders(['x-idempotency-key' => 'prod-42'])
    ->make('{"id":42,"name":"Laptop"}');
```

`withTopicKey()` accepts either a logical key or a full topic name — `ConnectionFaker` resolves it via `TopicRegistry`. Full method list — in [Testing the Consumer](/docs/1.x/consumer/testing#messagefactory).

### A Complete Consumer Test

```php
it('stores a product when the message is received', function () {
    KafkaBus::fake();

    KafkaBus::addMessage(
        MessageFactory::for()
            ->withTopicKey('products')
            ->withHeaders(['x-idempotency-key' => 'prod-42'])
            ->make('{"id":42,"name":"Laptop","status":"active"}')
    );

    KafkaBus::listen('products');

    expect(Product::find(42))->not->toBeNull();
    expect(Product::find(42)->name)->toBe('Laptop');
});
```

`listen()` takes a worker name (not a topic name) and returns on its own once the fake connection runs out of messages — it won't block.

### Multiple Messages

```php
$factory = MessageFactory::for()->withTopicKey('products');

KafkaBus::addMessage($factory->withOffset(0)->make('{"id":1}'));
KafkaBus::addMessage($factory->withOffset(1)->make('{"id":2}'));
KafkaBus::addMessage($factory->withOffset(2)->make('{"id":3}'));

KafkaBus::listen('products');

expect(Product::count())->toBe(3);
```

### Testing Multiple Workers <Badge type="info" text="laravel-bridge ^1.2" />

Starting with `kafka-bus/laravel-bridge:^1.2`, `listen()` accepts an array of worker names. Their topics are merged into a single consumer stream — the same way `kafka:consume worker1 worker2` behaves.

```php
it('processes messages from the products and orders workers', function () {
    KafkaBus::fake();

    KafkaBus::addMessage(
        MessageFactory::for()->withTopicKey('products')->make('{"id":1}')
    );
    KafkaBus::addMessage(
        MessageFactory::for()->withTopicKey('orders')->make('{"id":2}')
    );

    KafkaBus::listen(['products', 'orders']);

    KafkaBus::assertCommitted('products');
    KafkaBus::assertCommitted('orders');
});
```

::: tip
Use different `offset` values: `msgId()` is built as `"{partition}-{offset}-{topic}"`, and with `ConsumerCommiterMiddleware` active, messages sharing the same offset will be treated as duplicates and skipped.
:::

### Messages for Multiple Topics

The fake connection keeps a single shared queue. You can mix messages from different topics — routing sorts them out to the right handlers:

```php
KafkaBus::addMessage(MessageFactory::for()->withTopicKey('products')->make('{"id":1}'));
KafkaBus::addMessage(MessageFactory::for()->withTopicKey('orders')->make('{"id":2}'));

KafkaBus::listen('default'); // a worker subscribed to both topics
```

::: warning
The worker must have a route for every topic in the queue. A message with no route throws a `RouteConsumerException`.
:::

### Testing Handler Errors

```php
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

## Asserting Commits

After `listen()`, every successfully processed message is recorded in the fake. This lets you confirm the handler ran and the offset was confirmed:

```php
KafkaBus::listen('products');

// At least one message was committed
KafkaBus::assertCommitted('products');

// Conditional assertion — the callback receives a ConsumerMessageInterface
KafkaBus::assertCommitted(
    'products',
    fn ($msg) => $msg->payload() === '{"id":1}'
              && $msg->headers()['x-idempotency-key'] === 'prod-42'
);

// An exact count
KafkaBus::assertCommittedTimes('products', 2);

// Nothing was committed (e.g. before listen() was called)
KafkaBus::assertNothingCommitted();
```

Every commit-related method takes a **topic key**, not a worker name, and resolves it via `TopicRegistry`. An unregistered key throws a `TopicCannotResolvedException`.

### Retrieving Committed Messages

```php
$committed = KafkaBus::getCommitted('products'); // list<ConsumerMessageInterface>

expect($committed[0]->payload())->toBe('{"id":42}');
expect($committed[0]->headers())->toHaveKey('x-idempotency-key');
expect($committed[0]->original()->offset)->toBe(0);
```

## Method Reference

| Method                                      | Purpose                                          |
|------------------------------------------------|-------------------------------------------------|
| `fake()`                                   | Activates the fake                                  |
| `assertPublished($class, ?$callback)`      | The message was published                            |
| `assertPublishedTimes($class, $times)`     | Published exactly N times                             |
| `assertNotPublished($class)`               | The message wasn't published                          |
| `assertNothingPublished()`                 | Nothing was published                                   |
| `getPublished($class)`                     | `list<ProducerMessage>` for the class                   |
| `allPublished()`                           | `list<ProducerMessage>` across every topic               |
| `addMessage(RdKafka\Message $message)`     | Queues a message for reading                              |
| `listen(string $workerName)`               | Runs the consumer pipeline                                |
| `assertCommitted($topicKey, ?$callback)`   | The message was committed                                 |
| `assertCommittedTimes($topicKey, $times)`  | Committed exactly N times                                  |
| `assertNothingCommitted()`                 | Nothing was committed                                       |
| `getCommitted($topicKey)`                  | `list<ConsumerMessageInterface>` for the topic              |

## Test Environment

Configure a `null` connection to run tests without a broker:

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

With `KafkaBus::fake()`, the connection doesn't matter — the fake intercepts calls before they ever reach it. The `null` driver is useful when the fake isn't active but tests still shouldn't connect to a real Kafka: publishes are silently discarded.

::: warning
You can't *read* on a `null` connection: creating a consumer throws a `ConsumerException`. Always use `KafkaBus::fake()` for consumer tests.
:::
