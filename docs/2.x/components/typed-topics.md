---
title: Typed topics
description: Send multiple message types through one Kafka topic and select a decoder and handler using the x-message-type header
---

# Typed topics

Use typed topics when several event formats share one Kafka topic. The producer writes an `x-message-type` header; the consumer uses it to select a message factory and callback. This guide configures two delivery events on one topic.

## Requirements

`kafka-bus/typed-topics` is included in the current `kafka-bus/kafka-bus` meta-package. For an installation with individual components, install it together with the JSON helpers used below:

```bash
composer require kafka-bus/typed-topics kafka-bus/messages
```

The component requires PHP 8.2+, `ext-rdkafka` through Core, Kafka Bus Core `^2.2`, and `psr/log`. Configure a [connection](/docs/2.x/configuration) before publishing.

## Define the message types

Implement `HasMessageType` and return a stable, non-empty type identifier. Both messages below use JSON payloads:

```php
namespace App\Kafka\Messages;

use KafkaBus\Messages\JsonMessage;
use KafkaBus\TypedTopics\HasMessageType;

final class DeliveryUpdated extends JsonMessage implements HasMessageType
{
    public const TYPE = 'delivery.updated';

    public function getMessageType(): string
    {
        return self::TYPE;
    }
}

final class DeliveryCompleted extends JsonMessage implements HasMessageType
{
    public const TYPE = 'delivery.completed';

    public function getMessageType(): string
    {
        return self::TYPE;
    }
}
```

Place each class in its own file under `app/Kafka/Messages/`. Implement Core's `HasKey` as well if the messages need a partition key; the type header does not set it.

## Route each type to a callback

Create `app/Kafka/Consumers/DeliveryTopicHandler.php`:

```php
namespace App\Kafka\Consumers;

use App\Kafka\Messages\DeliveryCompleted;
use App\Kafka\Messages\DeliveryUpdated;
use KafkaBus\Core\Receivers\Attributes\MessageFactory;
use KafkaBus\Messages\Factories\JsonMessageFactory;
use KafkaBus\TypedTopics\MessageRoutes;
use KafkaBus\TypedTopics\TypedTopicHandler;

final class DeliveryTopicHandler extends TypedTopicHandler
{
    protected function routes(MessageRoutes $routes): void
    {
        $routes
            ->on(DeliveryUpdated::TYPE, $this->onUpdated(...))
            ->on(DeliveryCompleted::TYPE, $this->onCompleted(...));
    }

    #[MessageFactory(new JsonMessageFactory(DeliveryUpdated::class))]
    private function onUpdated(DeliveryUpdated $message): void
    {
        // Apply the delivery update.
    }

    #[MessageFactory(new JsonMessageFactory(DeliveryCompleted::class))]
    private function onCompleted(DeliveryCompleted $message): void
    {
        // Apply the delivery result.
    }
}
```

Each callback receives the decoded value as its sole argument. You can pass a factory explicitly as the third argument to `on()` instead of using an attribute. Explicit factories take precedence; otherwise factory selection uses the callback attribute, then the first parameter type (`string`, `array`, or `RdKafka\Message`), and finally the native consumer message. Registering the same type twice raises `InvalidArgumentException`.

`TypedTopicHandler` provides the topic's message factory through `ProvidesMessageFactory`. Decoding happens before the receiver middleware runs, so `formatted()` contains the selected DTO. Register the handler object itself, rather than a closure wrapping its `__invoke()` method, to preserve factory discovery.

## Connect the routes to the bus

With an existing `$connection`, register both publisher classes against the same topic and attach the typed handler:

```php
use App\Kafka\Consumers\DeliveryTopicHandler;
use App\Kafka\Messages\DeliveryCompleted;
use App\Kafka\Messages\DeliveryUpdated;
use KafkaBus\Core\Bus;
use KafkaBus\Core\Publishers\PublisherFactory;
use KafkaBus\Core\Publishers\Routing\Options;
use KafkaBus\Core\Publishers\Routing\PublisherRoutesBuilder;
use KafkaBus\Core\Receivers\Routing\ReceiverBuilder;
use KafkaBus\Core\Receivers\Routing\RouteInfo;
use KafkaBus\Core\Topics\Topic;
use KafkaBus\Core\Topics\TopicRegistry;
use KafkaBus\TypedTopics\PublisherTypedTopicMiddleware;

$topics = (new TopicRegistry())->add(new Topic('delivery.events', 'delivery'));
$options = new Options(middleware: [new PublisherTypedTopicMiddleware()]);

$publisherRoutes = PublisherRoutesBuilder::make($topics)
    ->add(DeliveryUpdated::class, 'delivery', $options)
    ->add(DeliveryCompleted::class, 'delivery', $options)
    ->build();
    
$receiver = ReceiverBuilder::make($topics)
    ->add(new RouteInfo('delivery', new DeliveryTopicHandler()))
    ->build();

$bus = new Bus($connection, new PublisherFactory(routes: $publisherRoutes), $receiver);

$bus->publish(DeliveryUpdated::from(['delivery_id' => 42]));
```

The record goes to `delivery.events` with `x-message-type: delivery.updated`. When a [worker](/docs/2.x/consumer/workers) dispatches it to this bus, `onUpdated()` receives a `DeliveryUpdated` instance.

::: warning Publisher middleware is required
Outside Laravel, attach `PublisherTypedTopicMiddleware` to every typed publisher route. Implementing `HasMessageType` alone does not write the header.
:::

## Configure Laravel

Using the same message and handler classes, merge these entries into `config/kafka-bus.php`:

```php
'topics' => [
    'delivery' => 'delivery.events',
],
'producers' => [
    'routes' => [
        App\Kafka\Messages\DeliveryUpdated::class => 'delivery',
        App\Kafka\Messages\DeliveryCompleted::class => 'delivery',
    ],
],
'consumers' => [
    'routes' => [
        'delivery' => App\Kafka\Consumers\DeliveryTopicHandler::class,
    ],
],
```

The current bridge includes `PublisherTypedTopicMiddleware` in its default publisher middleware and resolves the topic handler through the container. Publish through the [facade](/docs/2.x/laravel/producers) and start the consumer:

```bash
php artisan kafka:consume delivery
```

## Handle records without a matching type

By default, records without a header or with an unknown type become `UnroutableMessage` values, pass through the receiver pipeline, and complete without a business callback. Offset commits follow the application's existing configuration.

Choose a policy inside `routes()`:

| Method                                         | Behavior                                                                        |
|------------------------------------------------|---------------------------------------------------------------------------------|
| `whenHeaderMissing($handler, $factory = null)` | Decodes records with an absent or null header using a legacy callback's factory |
| `onUnroutableLog($logger)`                     | Logs an error through a PSR logger, then completes processing                   |
| `onUnroutableThrow()`                          | Throws `UnroutableMessageException` through the normal receiver error path      |
| `onUnroutable($handler)`                       | Passes an `UnroutableMessage` to a custom callback                              |

For example, add these calls after the typed routes to accept older records while rejecting unknown types:

```php
$routes
    ->whenHeaderMissing($this->onUpdated(...))
    ->onUnroutableThrow();
```

An empty or unknown header does not use the legacy fallback. `UnroutableMessage` exposes the original consumer message as `message` and the reason as `reason` (`MissingHeader` or `UnknownType`). Read the received header from `$unroutable->message->headers()` when your callback needs it.

Factory and business-callback exceptions follow the normal [receiver error handling](/docs/2.x/consumer/pipeline). The unroutable policy does not catch decoding or application errors.

## Testing

For the JSON messages above, use `PayloadTestFactory` from `kafka-bus/messages`. It creates the flat JSON payload expected by `JsonMessageFactory`. The factories use Faker; add it as a development dependency if it is not installed:

```bash
composer require --dev fakerphp/faker
```

```php
namespace Tests\Factories;

use App\Kafka\Messages\DeliveryUpdated;
use KafkaBus\Messages\Testing\PayloadTestFactory;
use KafkaBus\TypedTopics\HasMessageType;

/**
 * @extends PayloadTestFactory<DeliveryUpdated>
 */
final class DeliveryUpdatedFactory extends PayloadTestFactory
{
    protected string $topicKey = 'delivery';

    protected string $payloadClass = DeliveryUpdated::class;

    /** @var array<string, string> */
    protected array $headers = [
        HasMessageType::HEADER_NAME => DeliveryUpdated::TYPE,
    ];

    public function definition(): array
    {
        return ['delivery_id' => $this->faker->numberBetween(1, 1000)];
    }
}
```

### Building an incoming record

Define the type header once in the factory’s `$headers` property. Call `make()` to obtain an `RdKafka\Message`. The `payload()` method returns a DTO and does not exercise incoming-record routing.

::: info Default headers
`TestFactory` applies `$headers` when the factory is created, so every `make()` call already includes `x-message-type`. The factory does not run the publisher pipeline or call `HasMessageType::getMessageType()` or `HasKey::getKey()`: define the type in `$headers` and supply the Kafka key through `withKey()`.
:::

```php
use Tests\Factories\DeliveryUpdatedFactory;

$factory = DeliveryUpdatedFactory::new()
    ->withKey('42');

$record = $factory->make(['delivery_id' => 42]);
```

With `$receiver` and `$topics` from the example above, pass the registry to `withTopicKey()` to resolve the logical key to the physical topic name:

```php
use KafkaBus\Core\Consumers\Messages\ConsumerMessage;

$record = $factory
    ->withTopicKey('delivery', $topics)
    ->make(['delivery_id' => 42]);

$receiver->dispatch(new ConsumerMessage($record));
```

The record is decoded into `DeliveryUpdated` and passed to `onUpdated()`. Assert the business handler's effect in your test, such as an updated database row or a call to an injected service.

### Running through Laravel Fake

With the routes from the Laravel section configured, test the full consumer loop. The fake resolves the logical topic key, including its prefix:

```php
use KafkaBus\Laravel\Facades\KafkaBus;
use Tests\Factories\DeliveryUpdatedFactory;

it('consumes a typed delivery update', function () {
    KafkaBus::fake();

    $record = DeliveryUpdatedFactory::new()
        ->withKey('42')
        ->withOffset(0)
        ->make(['delivery_id' => 42]);

    KafkaBus::addMessage($record);
    KafkaBus::listen('delivery');

    KafkaBus::assertCommittedTimes('delivery', 1);
});
```

The commit assertion confirms processing completed, but does not prove which callback ran: unknown types also complete successfully by default. Add an assertion for the effect of `onUpdated()`.

For `DeliveryCompleted`, use a separate factory with `payloadClass = DeliveryCompleted::class` and `DeliveryCompleted::TYPE` in `$headers`. Give records in the same partition distinct offsets so duplicate protection does not skip the next record.

Also test a record without a header and one with an unknown type: they should follow your `whenHeaderMissing()` / `onUnroutable()` policy. `withHeaders()` replaces the entire header map, so supply the type and any other required headers together.


Override headers with `withHeaders()` for individual test scenarios:

```php
use KafkaBus\TypedTopics\HasMessageType;
use Tests\Factories\DeliveryUpdatedFactory;

$withoutType = DeliveryUpdatedFactory::new()->withHeaders([])->make();
$unknownType = DeliveryUpdatedFactory::new()
    ->withHeaders([HasMessageType::HEADER_NAME => 'delivery.unknown'])
    ->make();
```

For `DomainMessage` subclasses, use `DomainMessageTestFactory`, which creates the `event`, `attributes`, and `dirty` envelope. Define its type header in the `$headers` property as well. See [test message factories](/docs/2.x/components/messages) and [Laravel testing](/docs/2.x/laravel/testing).
