# Testing

`kafka-bus/core` provides fakes that let you verify producer behaviour without a real Kafka broker.

## ProducerMessageFaker

`ProducerMessageFaker` is a lightweight `ProducerMessageInterface` implementation for tests. It implements `HasHeaders` and `HasPartition`, so you can control every dimension of the outgoing message.

```php
use KafkaBus\Core\Testing\Messages\ProducerMessageFaker;

$message = new ProducerMessageFaker(
    message:   'test-message',       // raw payload string
    headers:   ['foo' => 'bar'],     // optional
    partition: 5,                    // optional, -1 = auto-assign
);
```

| Parameter    | Type     | Default | Description                              |
|--------------|----------|---------|------------------------------------------|
| `$message`   | `string` | —       | Raw payload returned by `toPayload()`    |
| `$headers`   | `array`  | `[]`    | Headers returned by `getHeaders()`       |
| `$partition` | `int`    | `-1`    | Partition number; `-1` lets Kafka decide |

## Full Producer Test

`ConnectionFaker` records every published message in memory. Use `ConnectionRegistryFaker` to wire it into the `Bus` instead of a real connection.

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

// 3. Build the Bus with fake connections
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

### Asserting Published Messages

After `publish()`, all sent messages are available in `$connectionFaker->publishedMessages`, keyed by the full topic name:

```php
$published = $connectionFaker->publishedMessages['production.fact.products.1'];

assert(count($published) === 1);

$message = $published[0]; // RdKafka-compatible message object

assert($message->payload   === 'test-message');
assert($message->partition === 5);
assert($message->headers   === ['foo' => 'bar']);
```

In a real test you can replace `ProducerMessageFaker` with your own `ProducerMessageInterface` implementation and assert that routing, payload serialization, and headers all behave as expected.