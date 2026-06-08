# Testing

The `KafkaBus` Facade ships with a first-class fake that works similarly to `Event::fake()` or `Mail::fake()`.

## Activating the Fake

```php
use KafkaBus\Laravel\Facades\KafkaBus;

KafkaBus::fake();
```

After this, all calls via the Facade (`publish`, `listen`) are intercepted by an in-memory fake. The real broker is not used.

## Asserting Published Messages

### Basic Assertion

```php
it('publishes a message when a product is created', function () {
    KafkaBus::fake();

    app(CreateProductAction::class)->execute(id: 1, name: 'Laptop');

    KafkaBus::assertPublished(ProductMessage::class);
});
```

### Assertion with a Condition

The callback receives a `ProducerMessage` after it has passed through the full middleware pipeline (including payload serialization and header injection):

```php
KafkaBus::assertPublished(
    ProductMessage::class,
    fn($msg) => str_contains($msg->payload, '"id":1')
             && isset($msg->headers['x-idempotency-key'])
);
```

### Asserting Count

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
// All messages of a given type (post-middleware, with payload and headers)
$messages = KafkaBus::getPublished(ProductMessage::class);

// All published messages
$all = KafkaBus::allPublished();

expect($messages[0]->payload)->toContain('"id":1');
expect($messages[0]->headers)->toHaveKey('x-idempotency-key');
```

## Testing a Consumer

`addMessage()` adds an `RdKafka\Message` to the fake connection, and `listen()` runs the full consumer pipeline — middleware, routing, handler, commit — without a real broker.

### Creating a Test Message

```php
use Micromus\KafkaBus\Testing\Consumers\MessageFactory;

$message = MessageFactory::for()
    ->withTopicKey('products')
    ->withHeaders(['x-idempotency-key' => 'prod-42'])
    ->make('{"id":42,"name":"Laptop"}');
```

### Full Consumer Test

```php
it('saves a product when a message is received', function () {
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

### Multiple Messages

```php
$factory = MessageFactory::for()->withTopicKey('products');

KafkaBus::addMessage($factory->make('{"id":1}'));
KafkaBus::addMessage($factory->make('{"id":2}'));
KafkaBus::addMessage($factory->make('{"id":3}'));

KafkaBus::listen('products');

expect(Product::count())->toBe(3);
```

## Asserting Commits

After `listen()`, every successfully processed message is recorded in the fake. This lets you verify that the handler ran and the offset was acknowledged:

```php
KafkaBus::listen('products');

// At least one message was committed
KafkaBus::assertCommitted('products');

// Assertion with a condition
KafkaBus::assertCommitted(
    'products',
    fn($msg) => $msg->payload() === '{"id":1}'
             && $msg->headers()['x-idempotency-key'] === 'prod-42'
);

// Exact count
KafkaBus::assertCommittedTimes('products', 2);

// Nothing committed (e.g. before calling listen)
KafkaBus::assertNothingCommitted();
```

### Retrieving Committed Messages

```php
$committed = KafkaBus::getCommitted('products'); // list<ConsumerMessageInterface>

expect($committed[0]->payload())->toBe('{"id":42}');
expect($committed[0]->headers())->toHaveKey('x-idempotency-key');
```

## Test Environment (.env.testing)

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

With `KafkaBus::fake()`, the connection does not matter — the fake intercepts calls before they reach it. The `null` driver is useful when the fake is not activated but tests still must not connect to a real Kafka.
