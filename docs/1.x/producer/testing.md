# Testing

`kafka-bus/core` ships fakes that let you verify producer behavior without a real Kafka broker.

::: tip Laravel
In a Laravel application there's nothing to wire up by hand — use `KafkaBus::fake()` with the ready-made `assertPublished()` / `assertPublishedTimes()` assertions. See [Testing in Laravel](/docs/1.x/laravel/testing).
:::

## ProducerMessageFaker

`ProducerMessageFaker` is a lightweight `ProducerMessageInterface` implementation for tests. It implements `HasHeaders` and `HasPartition`, so you can control every parameter of the outgoing message.

```php
use KafkaBus\Core\Testing\Messages\ProducerMessageFaker;

$message = new ProducerMessageFaker(
    message:   'test-message',       // payload string
    headers:   ['foo' => 'bar'],     // optional
    partition: 5,                    // optional, -1 = auto-select
);
```

| Parameter    | Type      | Default | Description                                        |
|---------------|-----------|-----------|----------------------------------------------------|
| `$message`   | `string`  | —         | Raw payload returned by `toPayload()`               |
| `$headers`   | `array`   | `[]`      | Headers returned by `getHeaders()`                   |
| `$partition` | `int`     | `-1`      | Partition number; `-1` means Kafka picks it automatically |

`ProducerMessageFaker` doesn't implement `HasKey`, so the published message's key is always `null`. If you need to test partition key logic, use your own message class.

## A Complete Producer Test

`ConnectionFaker` records every published message in memory. `ConnectionRegistryFaker` wires it into `Bus` in place of a real connection — and it returns the same connection for any name, so `onConnection()` works in tests too.

```php
use KafkaBus\Core\Bus;
use KafkaBus\Core\Bus\Listeners\ListenerFactory;
use KafkaBus\Core\Bus\Publishers\PublisherFactory;
use KafkaBus\Core\Bus\Publishers\Router\PublisherRoutes;
use KafkaBus\Core\Bus\Publishers\Router\Route;
use KafkaBus\Core\Bus\ThreadFactory;
use KafkaBus\Core\Bus\ThreadRegistry;
use KafkaBus\Core\Consumers\ConsumerStreamFactory;
use KafkaBus\Core\Producers\ProducerStreamFactory;
use KafkaBus\Core\Testing\Connections\ConnectionFaker;
use KafkaBus\Core\Testing\Connections\ConnectionRegistryFaker;
use KafkaBus\Core\Testing\Messages\ProducerMessageFaker;
use KafkaBus\Core\Topics\Topic;
use KafkaBus\Core\Topics\TopicRegistry;

// 1. Register topics
$topicRegistry = (new TopicRegistry())
    ->add(new Topic('production.fact.products.1', 'products'));

$connectionFaker = new ConnectionFaker($topicRegistry);

// 2. Map message classes to topics
$routes = (new PublisherRoutes())
    ->add(new Route(ProducerMessageFaker::class, $topicRegistry->get('products')));

// 3. Assemble Bus with fake connections
$bus = new Bus(
    new ThreadRegistry(
        new ConnectionRegistryFaker($connectionFaker),
        new ThreadFactory(
            new ListenerFactory(
                new ConsumerStreamFactory(),
            ),
            new PublisherFactory(
                new ProducerStreamFactory(),
                $routes
            ),
        )
    ),
    'default'
);

// 4. Publish a message
$bus->publish(new ProducerMessageFaker('test-message', ['foo' => 'bar'], 5));
```

::: tip
Routes here are built directly via `new Route(...)`, bypassing `PublisherRoutesBuilder`. That's convenient in tests: the topic key doesn't need to be registered in the registry — a ready-made `Topic` object is enough.
:::

### Asserting Published Messages

After `publish()`, every sent message is available in `$connectionFaker->publishedMessages`, keyed by the full topic name:

```php
$published = $connectionFaker->publishedMessages['production.fact.products.1'];

assert(count($published) === 1);

$message = $published[0]; // KafkaBus\Core\Producers\Messages\ProducerMessage

assert($message->payload   === 'test-message');
assert($message->partition === 5);
assert($message->headers   === ['foo' => 'bar']);
assert($message->key       === null);
assert($message->topic->key === 'products');
```

::: warning
`publishedMessages` holds `ProducerMessage` objects — the result of the **entire** pipeline, not the application's original messages. That's useful: the test sees exactly what would have gone to the broker, including headers added by middleware. But it's not an `RdKafka\Message` — it has no `offset`, `err`, or other broker fields.
:::

Available `ProducerMessage` properties (all `readonly`):

| Property    | Type                     | Description                              |
|--------------|----------------------------|---------------------------------------------|
| `topic`     | `Topic`                  | The target topic                              |
| `payload`   | `string`                 | The result of `toPayload()`                    |
| `headers`   | `array<string, mixed>`   | Final headers                                  |
| `partition` | `int`                    | `-1` if no partition was set explicitly        |
| `key`       | `?string`                | The partition key                              |

## Asserting Middleware

Since `publishedMessages` holds the result of the entire pipeline, middleware are verified without any extra setup — just add them to the route's `Options`:

```php
use KafkaBus\Core\Bus\Publishers\Router\Options;
use KafkaBus\Commiter\Middleware\ProducerIdempotencyMiddleware;

$routes = (new PublisherRoutes())
    ->add(new Route(
        OrderMessage::class,
        $topicRegistry->get('orders'),
        new Options(middleware: [new ProducerIdempotencyMiddleware()])
    ));

$bus->publish(new OrderMessage(orderId: 42));

$published = $connectionFaker->publishedMessages['production.fact.orders.1'];

assert($published[0]->headers['x-idempotency-key'] === 'order-42');
```

Canceling a publish is verified the same way: if the middleware never called `continue()`, the `publishedMessages` array for that topic stays empty.

## Asserting Nothing Was Published

`ConnectionFaker::$publishedMessages` is a plain public array, so negative assertions are just as direct:

```php
assert($connectionFaker->publishedMessages === []);                  // nothing was published
assert(! isset($connectionFaker->publishedMessages['other.topic'])); // nothing was written to this topic
```

In a real test, use your own `ProducerMessageInterface` implementation instead of `ProducerMessageFaker`, and check routing, payload serialization, and header composition.
