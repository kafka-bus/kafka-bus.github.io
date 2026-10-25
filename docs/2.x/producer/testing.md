---
title: Testing the producer
description: Verifying publishes without a Kafka broker using BusFaker and its assertions
---

# Testing the producer

`kafka-bus/core` ships `BusFaker` — a `BusInterface` implementation that records everything published instead of sending it to Kafka, and provides ready-made assertions. Routes, topics, and middleware stay real; only the transport is replaced.

::: tip Laravel
In a Laravel application there's nothing to wire up by hand — call `KafkaBus::fake()` and use the same assertions through the facade. See [Testing in Laravel](/docs/2.x/laravel/testing).
:::

## Creating the fake bus

`BusFaker::make()` takes the topic registry, an assertion driver, and — optionally — the same `PublisherFactory` you use in production:

```php
use KafkaBus\Core\Publishers\PublisherFactory;
use KafkaBus\Core\Publishers\Routing\PublisherRoutesBuilder;
use KafkaBus\Core\Testing\Assertions\PHPUnitAssertionDriver;
use KafkaBus\Core\Testing\BusFaker;
use KafkaBus\Core\Topics\Topic;
use KafkaBus\Core\Topics\TopicRegistry;

$topicRegistry = (new TopicRegistry())
    ->add(new Topic('production.fact.products.1', 'products'));

$routes = PublisherRoutesBuilder::make($topicRegistry)
    ->add(ProductCreatedMessage::class, 'products')
    ->build();

$bus = BusFaker::make(
    $topicRegistry,
    new PHPUnitAssertionDriver(),
    new PublisherFactory(routes: $routes),
);
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `topicRegistry` | `TopicRegistry` | — | Resolves topic keys in assertions |
| `driver` | `AssertionDriverInterface` | — | How a failed assertion is reported |
| `publisherFactory` | `PublisherFactory` | `new PublisherFactory()` | Producer routes |
| `receiver` | `ReceiverInterface` | `new Receiver()` | Consumer routes, see [Testing the consumer](/docs/2.x/consumer/testing) |

Pass `$bus` wherever your code expects a `BusInterface`.

### Assertion drivers

The driver decides which test framework receives a failed assertion:

| Driver | Framework |
|---|---|
| `PHPUnitAssertionDriver` | PHPUnit and Pest — throws `ExpectationFailedException` |
| `TestoAssertionDriver` | Testo — throws Testo's `AssertionException` |

For another framework, implement `AssertionDriverInterface` — two methods, `notEmpty()` and `count()`.

## Asserting published messages

```php
$bus->publish(new ProductCreatedMessage(1, 'Laptop', 'Electronics'));

$bus->assertPublished(ProductCreatedMessage::class);
$bus->assertPublishedTimes(ProductCreatedMessage::class, 1);
```

### Conditional assertion

The callback receives a `ProducerMessage` — the result of the whole pipeline, including serialization and headers added by middleware:

```php
use KafkaBus\Core\Producers\Messages\ProducerMessage;

$bus->assertPublished(
    ProductCreatedMessage::class,
    static fn (ProducerMessage $message): bool => $message->key === '1'
        && $message->headers['event'] === 'product.created',
);
```

Available `ProducerMessage` properties (all `readonly`):

| Property | Type | Description |
|---|---|---|
| `topic` | `Topic` | The target topic |
| `payload` | `string` | The result of `toPayload()` |
| `headers` | `array<string, mixed>` | Final headers |
| `partition` | `int` | `-1` if no partition was set explicitly |
| `key` | `?string` | The partition key |

### Asserting nothing was published

```php
$bus->assertNotPublished(ProductCreatedMessage::class);
$bus->assertNothingPublished();
```

### Retrieving published messages

```php
$messages = $bus->getPublished(ProductCreatedMessage::class); // list<ProducerMessage>
$all = $bus->allPublished();                                   // every topic
```

::: warning Messages are grouped by topic
`getPublished()` resolves the topic from the message class via the route table and returns every message on that topic. If several message classes route to the same topic, all of them are in the result.

For a class without a route, the method returns an empty array — so `assertNotPublished()` for it always passes.
:::

## Asserting middleware

`BusFaker` records messages after the pipeline, so middleware is checked without any extra setup — add it to the route's `Options`:

```php
use KafkaBus\Commiter\Middleware\PublisherIdempotencyMiddleware;
use KafkaBus\Core\Publishers\Routing\Options;

$routes = PublisherRoutesBuilder::make($topicRegistry)
    ->add(OrderMessage::class, 'orders', new Options(
        middleware: [new PublisherIdempotencyMiddleware()],
    ))
    ->build();

$bus = BusFaker::make($topicRegistry, new PHPUnitAssertionDriver(), new PublisherFactory(routes: $routes));

$bus->publish(new OrderMessage(orderId: 42));

$bus->assertPublished(
    OrderMessage::class,
    static fn (ProducerMessage $message): bool => $message->headers['x-idempotency-key'] === 'order-42',
);
```

A canceled send is checked the same way: if the middleware didn't call `continue()`, `assertNotPublished()` passes.

## Test message class

`ProducerMessageFaker` is a lightweight `ProducerMessageInterface` implementation for tests. It implements `HasHeaders` and `HasPartition`:

```php
use KafkaBus\Core\Testing\Messages\ProducerMessageFaker;

$message = new ProducerMessageFaker(
    message: 'test-message',
    headers: ['foo' => 'bar'],
    partition: 5,
);
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `message` | `string` | — | Raw payload returned by `toPayload()` |
| `headers` | `array` | `[]` | Headers returned by `getHeaders()` |
| `partition` | `int` | `-1` | Partition number; `-1` lets Kafka choose |

It doesn't implement `HasKey`, so the key is always `null`. In real tests, prefer your own message classes — that way the test also covers serialization.

## Low-level access

`BusFaker` is built on `ConnectionFaker`, which stores published messages in a public array keyed by the physical topic name. You can read it through the fake's connection:

```php
/** @var \KafkaBus\Core\Testing\Connections\ConnectionFaker $connection */
$connection = $bus->connection();

$connection->publishedMessages['production.fact.products.1']; // list<ProducerMessage>
```
