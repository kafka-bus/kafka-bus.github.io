---
title: Upgrading from 1.x
description: Breaking changes in Kafka Bus 2.0 and a step-by-step guide to migrating an application from 1.x
---

# Upgrading from 1.x

Kafka Bus 2.0 reworks how messages are received. In 1.x, a `Listener` read Kafka on its own and routed messages past `Bus`. In 2.x, `Bus` is the single entry point in both directions: publishing goes through `Bus::publish()`, receiving goes through `Bus::dispatch()`, and reading from Kafka moved to a separate package, `kafka-bus/worker`.

This page lists every breaking change and shows the "before → after" code for each one. Read it top to bottom when upgrading a plain PHP application, or jump to [Laravel Bridge 2.0](#laravel-bridge-2-0) if you use the Laravel integration.

## What changed at a glance

| Area | 1.x | 2.x |
|---|---|---|
| Packages | `core`, `messages`, `commiter` | plus `worker` and `metadata`; meta-package `kafka-bus/kafka-bus` |
| Connections | `Bus` holds many connections, `onConnection()` | one `Bus` = one connection |
| Consumer routes | stored inside each `Worker` | stored in `Bus` (a `Receiver`), one handler per topic |
| Reading Kafka | `$bus->listener('name')->listen()` | `WorkerRunnerFactory::create($bus, 'name')->run()` |
| Consumer middleware | two levels: worker and route | one level: route (`ReceiverPipelineMiddleware`) |
| Offsets | `Listener::partitions()` | `kafka-bus/metadata`: `Metadata::fromConnection()` |
| Testing | `ConnectionRegistryFaker` + manual asserts | `BusFaker` with built-in assertions |
| Namespaces | `KafkaBus\Core\Interfaces\…`, `Bus\…` | contracts live next to their implementations |

## Dependencies

All five packages are released together under the same tag. The simplest option is the meta-package, which replaces `core`, `messages`, `commiter`, `worker`, and `metadata`:

```bash
composer require kafka-bus/kafka-bus:^2.1.1
```

If you install packages one by one, bump all of them to `^2.0` and add the new ones you need:

```json
{
    "require": {
        "kafka-bus/core": "^2.0",
        "kafka-bus/messages": "^2.0",
        "kafka-bus/commiter": "^2.0",
        "kafka-bus/worker": "^2.0",
        "kafka-bus/metadata": "^2.0"
    }
}
```

`kafka-bus/worker` is required to read from Kafka, `kafka-bus/metadata` is required to view or reset offsets. A publishing-only service needs just `kafka-bus/core`.

## Namespace changes

The `KafkaBus\Core\Interfaces\…` tree is gone — each contract now lives next to its implementation. The `Bus\…` subtree is split into `Publishers\…` and `Receivers\…`.

::: details Full class map for kafka-bus/core
| 1.x | 2.x |
|---|---|
| `Interfaces\Bus\BusInterface` | `BusInterface` |
| `Interfaces\Connections\ConnectionInterface` | `Connections\ConnectionInterface` |
| `Interfaces\Connections\ConnectionConfigInterface` | `Connections\Config\ConnectionConfigInterface` |
| `Interfaces\Connections\ConnectionRegistryInterface` | `Connections\Registry\ConnectionRegistryInterface` |
| `Interfaces\Connections\SaslConfigurationConfigInterface` | `Connections\Config\SaslConfigurationConfigInterface` |
| `Interfaces\Consumers\ConsumerInterface` | `Consumers\ConsumerInterface` |
| `Interfaces\Consumers\Messages\ConsumerMessageInterface` | `Consumers\Messages\ConsumerMessageInterface` |
| `Interfaces\Consumers\Messages\MessageFactoryInterface` | `Receivers\Messages\MessageFactoryInterface` |
| `Interfaces\Producers\ProducerInterface` | `Producers\ProducerInterface` |
| `Interfaces\Producers\Messages\ProducerMessageInterface` | `Producers\Messages\ProducerMessageInterface` |
| `Interfaces\Producers\Messages\HasKey` | `Producers\Messages\HasKey` |
| `Interfaces\Producers\Messages\HasHeaders` | `Producers\Messages\HasHeaders` |
| `Interfaces\Producers\Messages\HasPartition` | `Producers\Messages\HasPartition` |
| `Interfaces\Pipelines\PipelineInterface` | `Pipelines\PipelineInterface` |
| `Interfaces\Pipelines\PipelineHandlerInterface` | `Pipelines\PipelineHandlerInterface` |
| `Interfaces\Pipelines\PipelineMiddlewareInterface` | `Pipelines\PipelineMiddlewareInterface` |
| `Bus\MessageBatch` | `Publishers\MessageBatch` |
| `Bus\Publishers\Publisher` | `Publishers\Publisher` |
| `Bus\Publishers\PublisherFactory` | `Publishers\PublisherFactory` |
| `Bus\Publishers\Router\PublisherRoutesBuilder` | `Publishers\Routing\PublisherRoutesBuilder` |
| `Bus\Publishers\Router\Route` | `Publishers\Routing\Route` |
| `Bus\Publishers\Router\Options` | `Publishers\Routing\Options` |
| `Bus\Publishers\Router\PublisherRoutes` | `Routing\RouteCollection` |
| `Producers\ProducerStream` | `Publishers\PublisherStream` |
| `Producers\ProducerStreamFactory` | `Publishers\PublisherStreamFactory` |
| `Producers\Pipelines\ProducerPipelineHandler` | `Publishers\Pipelines\PublisherPipelineHandler` |
| `Producers\Pipelines\ProducerPipelineMiddleware` | `Publishers\Pipelines\PublisherPipelineMiddleware` |
| `Consumers\Router\ConsumerRoutesBuilder` | `Receivers\Routing\ReceiverRoutesBuilder` (or `ReceiverBuilder`) |
| `Consumers\Router\ConsumerRoutes` | `Routing\RouteCollection` |
| `Consumers\Router\Route` | `Receivers\Routing\Route` |
| `Consumers\Router\RouteInfo` | `Receivers\Routing\RouteInfo` |
| `Consumers\Router\RouteExecutor` | `Receivers\Routing\RouteExecutor` |
| `Consumers\Attributes\MessageFactory` | `Receivers\Attributes\MessageFactory` |
| `Consumers\Messages\NativeMessageFactory` | `Receivers\Messages\NativeMessageFactory` |
| `Consumers\Messages\StringMessageFactory` | `Receivers\Messages\StringMessageFactory` |
| `Consumers\Messages\JsonMessageFactory` | `Receivers\Messages\JsonMessageFactory` |
| `Consumers\Messages\OriginalMessageFactory` | `Receivers\Messages\OriginalMessageFactory` |
| `Consumers\Pipelines\ConsumerPipelineHandler` | `Receivers\Pipelines\ReceiverPipelineHandler` |
| `Consumers\Pipelines\ConsumerPipelineMiddleware` | `Receivers\Pipelines\ReceiverPipelineMiddleware` |
| `Consumers\Pipelines\MessagePipelineMiddleware` | `Receivers\Pipelines\ReceiverPipelineMiddleware` |
| `Support\RetryRepeater` | `Utils\RetryRepeater` |
:::

::: details Classes moved to kafka-bus/worker
| 1.x (`KafkaBus\Core\…`) | 2.x (`KafkaBus\Worker\…`) |
|---|---|
| `Bus\Listeners\Workers\Worker` | `Worker` |
| `Bus\Listeners\Workers\Options` | `Options` |
| `Bus\Listeners\Workers\MemoryWorkerRegistry` | `Registry\MemoryWorkerRegistry` |
| `Interfaces\Bus\Listeners\WorkerRegistryInterface` | `Registry\WorkerRegistryInterface` |
| `Bus\Listeners\Workers\WorkerMerger` | `WorkerMerger` |
| `Consumers\ConsumerStream` | `Consumers\ConsumerStream` |
| `Interfaces\Consumers\ConsumerStreamInterface` | `Consumers\ConsumerStreamInterface` |
| `Exceptions\Listeners\ListenerException` | `Exceptions\WorkerException` |
:::

::: details Classes moved to kafka-bus/metadata
| 1.x (`KafkaBus\Core\…`) | 2.x (`KafkaBus\Metadata\…`) |
|---|---|
| `Bus\Listeners\Partitions\Partitions` | `Partitions\Partitions` |
| `Interfaces\Bus\Listeners\PartitionsInterface` | `Partitions\PartitionsInterface` |
| `Bus\Listeners\Partitions\CommitOffset` | `Partitions\CommitOffset` |
| `Bus\Listeners\Partitions\CommitOffsetResult` | `Partitions\CommitOffsetResult` |
| `Bus\Listeners\Partitions\Offset` | `Partitions\Offset` |
| `Bus\Listeners\Partitions\TopicPartition` | `Partitions\TopicPartition` |
| `Connections\Topics\Topics` | `Topics\TopicsMetadata` |
| `Exceptions\Listeners\CannotCommitOffsetException` | `Exceptions\CannotCommitOffsetException` |
| `Exceptions\TopicNotFoundException` | `Exceptions\TopicNotFoundException` |
:::

Removed without a replacement: `Thread`, `ThreadFactory`, `ThreadRegistry`, `ThreadInterface`, `Listener`, `ListenerFactory`, `ListenerInterface`, `MessageHandler`, `MessageHandlerFactory`, `RouteHandler`, `WorkerConsumerMessage`, `ConsumerMeta`, `ConnectionHasTopicsInterface`, `ConnectionRegistryFaker`.

`kafka-bus/messages` keeps its own namespaces — only its imports of core classes changed.

## Building the bus

`Bus` no longer takes a `ThreadRegistry`. It is bound to a single connection and receives the publisher routes and the receiver directly:

```php
use KafkaBus\Core\Bus;

$bus = new Bus( // [!code --]
    new Bus\ThreadRegistry( // [!code --]
        $connectionRegistry, // [!code --]
        new Bus\ThreadFactory( // [!code --]
            new Bus\Listeners\ListenerFactory(workerRegistry: $workerRegistry), // [!code --]
            new Bus\Publishers\PublisherFactory(routes: $publisherRoutes), // [!code --]
        ), // [!code --]
    ), // [!code --]
    defaultConnection: 'kafka', // [!code --]
); // [!code --]
$bus = new Bus( // [!code ++]
    connection: $connectionRegistry->connection('kafka'), // [!code ++]
    publisherFactory: new PublisherFactory(routes: $publisherRoutes), // [!code ++]
    receiver: $receiver, // [!code ++]
); // [!code ++]
```

`ConnectionRegistry`, `KafkaConnectionConfig`, `UserCredentialsConfig`, and `DriverRegistry` work as before. See [Configuration](/docs/2.x/configuration#building-the-bus).

### Multiple connections

`Bus::onConnection()` is removed. To publish to a second cluster, build a second `Bus` on that connection:

```php
$bus->onConnection('analytics')->publish($message); // [!code --]
$analyticsBus = new Bus($connectionRegistry->connection('analytics'), $publisherFactory); // [!code ++]
$analyticsBus->publish($message); // [!code ++]
```

## Publishing

The public API (`publish()`, `publishBatch()`, `MessageBatch`) is unchanged apart from namespaces. Two things moved:

- `Bus::routes()` is removed — use `$bus->publisher()->routes()` (available on the `Bus` class, not on `BusInterface`);
- `PublisherRoutesBuilder::build()` returns a `RouteCollection` instead of `PublisherRoutes`.

Producer middleware must implement the renamed interface:

```php
use KafkaBus\Core\Producers\Pipelines\ProducerPipelineHandler; // [!code --]
use KafkaBus\Core\Producers\Pipelines\ProducerPipelineMiddleware; // [!code --]
use KafkaBus\Core\Publishers\Pipelines\PublisherPipelineHandler; // [!code ++]
use KafkaBus\Core\Publishers\Pipelines\PublisherPipelineMiddleware; // [!code ++]

final class TracingMiddleware implements ProducerPipelineMiddleware // [!code --]
final class TracingMiddleware implements PublisherPipelineMiddleware // [!code ++]
```

The handler methods (`target()`, `topic()`, `withHeader()`) are the same.

## Receiving messages

### Routes live in the bus

In 1.x, each `Worker` carried its own route table. In 2.x, routes belong to the `Receiver` inside `Bus`, and a `Worker` only lists the topics to poll:

```php
// 1.x
$consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', new ProductHandler()))
    ->build();

$workerRegistry = MemoryWorkerRegistry::make()
    ->add(new Worker('products', $consumerRoutes, new Options(/* ... */)));
```

```php
// 2.x
use KafkaBus\Core\Receivers\Routing\ReceiverBuilder;
use KafkaBus\Core\Receivers\Routing\RouteInfo;
use KafkaBus\Worker\Options;
use KafkaBus\Worker\Registry\MemoryWorkerRegistry;
use KafkaBus\Worker\Worker;

$receiver = ReceiverBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', new ProductHandler()))
    ->build();

$workerRegistry = MemoryWorkerRegistry::make()
    ->add(new Worker('products', [$topicRegistry->get('products')], new Options(/* ... */)));
```

::: warning One handler per topic
Routes are keyed by the physical topic name inside a single `Bus`. In 1.x, two workers could bind different handlers to the same topic. In 2.x, the second `add()` for the same topic replaces the first. To process one topic in two different ways, run two applications (or two `Bus` instances) with different `group.id`s, or dispatch to both handlers from one.
:::

### Starting a worker

`Bus::listener()` is removed. The polling loop is created by `WorkerRunnerFactory` from `kafka-bus/worker`:

```php
$listener = $bus->listener('products'); // [!code --]
$listener->listen(); // [!code --]
$runner = (new WorkerRunnerFactory($workerRegistry))->create($bus, 'products'); // [!code ++]
$runner->run(); // [!code ++]
```

`forceStop()` moved from `Listener` to `WorkerRunner`. Merging several workers into one process (`create($bus, ['products', 'orders'])`) still works — now it merges only topic lists. See [Workers](/docs/2.x/consumer/workers).

### Middleware

The two consumer middleware levels are merged into one. Both `ConsumerPipelineMiddleware` (worker level) and `MessagePipelineMiddleware` (route level) become `ReceiverPipelineMiddleware`, attached to a route:

| What you had in 1.x | What to use in 2.x |
|---|---|
| `Workers\Options::$middleware` | `RouteInfo::$middleware` on every route that needs it |
| `ConsumerPipelineHandler::target()` — raw message | `ReceiverPipelineHandler::target()` — raw message |
| `MessagePipelineHandler::target()` — deserialized object | `ReceiverPipelineHandler::formatted()` — deserialized object |

```php
final class CheckingMiddleware implements MessagePipelineMiddleware // [!code --]
final class CheckingMiddleware implements ReceiverPipelineMiddleware // [!code ++]
{
    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        $order = $pipeline->handler()->target(); // [!code --]
        $order = $pipeline->handler()->formatted(); // [!code ++]

        // ...
    }
}
```

Worker-level middleware such as logging or `ConsumerCommiterMiddleware` must now be listed in each route. See [Consumer pipeline](/docs/2.x/consumer/pipeline).

::: tip
Exceptions thrown by the handler now reach route middleware **unwrapped**. `Receiver::dispatch()` wraps them in `MessageConsumerNotHandledException` only after the pipeline has finished.
:::

## Offset commit mode

The meaning of `autoCommit` changed in `kafka-bus/core` 1.3.1 and 2.1.1 — the commiters used to be swapped:

| `autoCommit` | 1.3.0 and earlier, 2.0.0–2.1.0 | 1.3.1+, 2.1.1+ |
|---|---|---|
| `true` | explicit `commitAsync()` after each message (plus librdkafka background commits) | librdkafka background commits only |
| `false` | nothing is committed | explicit `commitAsync()` after each message |

If you relied on "commit after each processed message" with `autoCommit: true`, set `autoCommit: false` after upgrading. In Laravel, `auto_commit` defaults to `false`, so the default now commits after each message. See [Committing offsets](/docs/2.x/consumer/workers#committing-offsets).

## Offsets

`Listener::partitions()` is replaced by the `kafka-bus/metadata` package. It doesn't depend on workers — you pass the topics and the consumer group explicitly:

```php
$partitions = $bus->listener('products')->partitions(); // [!code --]
$partitions = Metadata::fromConnection($connection) // [!code ++]
    ->partitions([$topicRegistry->get('products')], new ConsumerConfig( // [!code ++]
        additionalOptions: ['group.id' => 'products-service'], // [!code ++]
    )); // [!code ++]

$partitions->list();
$partitions->setOffset(new CommitOffset($topic, RD_KAFKA_PARTITION_UA, Offset::Early));
```

`list()` and `setOffset()` keep their behavior. See [Metadata](/docs/2.x/components/metadata).

## Commiter

| 1.x | 2.x |
|---|---|
| `ProducerIdempotencyMiddleware` | `PublisherIdempotencyMiddleware` |
| `ConsumerCommiterMiddleware` on a worker | `ConsumerCommiterMiddleware` on a route |

`ConsumerCommiterMiddleware` now implements `ReceiverPipelineMiddleware`; its constructor and behavior are unchanged. Repositories and sources are unchanged.

## Testing

`ConnectionRegistryFaker` is removed, and `ConnectionFaker` no longer takes a `TopicRegistry`. Build a `BusFaker` instead — it wires a fake connection and a recording receiver, and ships assertions:

```php
use KafkaBus\Core\Testing\Assertions\PHPUnitAssertionDriver;
use KafkaBus\Core\Testing\BusFaker;

$bus = BusFaker::make(
    $topicRegistry,
    new PHPUnitAssertionDriver(),
    new PublisherFactory(routes: $publisherRoutes),
    $receiver,
);

$bus->publish(new ProductMessage(/* ... */));

$bus->assertPublished(ProductMessage::class);
```

`MessageFactory::withTopicKey()` gained an optional second argument, a `TopicRegistry`, to resolve the key into a physical name. See [Testing the producer](/docs/2.x/producer/testing) and [Testing the consumer](/docs/2.x/consumer/testing).

## Exceptions

| 1.x | 2.x |
|---|---|
| `ListenerException` — worker not found | `KafkaBus\Worker\Exceptions\WorkerException` |
| `Core\Exceptions\Listeners\CannotCommitOffsetException` | `KafkaBus\Metadata\Exceptions\CannotCommitOffsetException` |
| `Core\Exceptions\TopicNotFoundException` | `KafkaBus\Metadata\Exceptions\TopicNotFoundException` |

The rest of the hierarchy is unchanged.

## Laravel Bridge 2.0

`kafka-bus/laravel-bridge:^2.0` requires the meta-package `kafka-bus/kafka-bus:^2.1` instead of separate `core`, `commiter`, and `messages`:

```bash
composer require kafka-bus/laravel-bridge:^2.0
```

### Configuration

Handlers moved out of workers into a new `consumers.routes` section, keyed by topic. Workers now list only topic keys and consumer options:

```php
// 1.x — config/kafka-bus.php
'consumers' => [
    'workers' => [
        'products' => App\Kafka\Consumers\ProductsConsumer::class,
        'default' => [
            'middleware' => [App\Kafka\Middleware\LoggingMiddleware::class],
            'topics' => [
                'orders' => [
                    'handler' => App\Kafka\Consumers\OrdersConsumer::class,
                    'middleware' => [App\Kafka\Middleware\TenantMiddleware::class],
                ],
            ],
        ],
    ],
],
```

```php
// 2.x — config/kafka-bus.php
'consumers' => [
    'routes' => [
        'products' => App\Kafka\Consumers\ProductsConsumer::class,
        'orders' => [
            'handler' => App\Kafka\Consumers\OrdersConsumer::class,
            'middleware' => [
                App\Kafka\Middleware\LoggingMiddleware::class,
                App\Kafka\Middleware\TenantMiddleware::class,
            ],
        ],
    ],

    'workers' => [
        'default' => [
            'topics' => ['orders'],
        ],
    ],
],
```

What changed in the `consumers` section:

- `routes` is new: `topic key => handler class` or `['handler' => …, 'middleware' => […]]`;
- a worker's `topics` is a list of topic keys, not a `topic => handler` map;
- the `handler`, `topic_key`, and `middleware` keys of a worker are no longer read;
- every route is also a worker of the same name — `kafka:consume products` works without a `workers` entry;
- the commented hint for producers now references `PublisherIdempotencyMiddleware`.

### Artisan commands

| 1.x | 2.x |
|---|---|
| `kafka:consume:all {--expect=*}` | `kafka:consume:routes {--expect=*}` — all routes in one process |
| `kafka:route:list` | `kafka:publisher:route:list` |
| — | `kafka:consumer:route:list` |
| `kafka:offset:show {worker}` | `kafka:consumer:offset:show {topicKey?}` |
| `kafka:offset:set {worker} {topic} {offset}` | `kafka:consumer:offset:set {topicKey} {offset}` |

The offset commands no longer take a worker — they use the consumer group from the global `consumers.additional_options`. See [Commands](/docs/2.x/laravel/commands).

### Facade and testing

- `KafkaBus::onConnection()`, `KafkaBus::routes()`, and `KafkaBus::listener()` are removed;
- `KafkaBus::connection()` and `KafkaBus::dispatch()` are added;
- the fake gains `assertDispatched()`, `assertDispatchedTimes()`, `assertNotDispatched()`, `assertNothingDispatched()`, `getDispatched()`, and `allDispatched()`;
- `KafkaBus::listen()` accepts a worker name or a list of names.

See [Testing in Laravel](/docs/2.x/laravel/testing).

### Upgrade checklist

1. Bump `kafka-bus/laravel-bridge` to `^2.0`, make sure `kafka-bus/kafka-bus` resolves to `2.1.1` or later, and remove explicit `kafka-bus/core`, `kafka-bus/commiter`, `kafka-bus/messages` requirements.
2. Move every handler from `consumers.workers` to `consumers.routes`.
3. Move worker middleware into each route's `middleware` (or into global `consumers.middleware`).
4. Replace `ProducerIdempotencyMiddleware` with `PublisherIdempotencyMiddleware`.
5. Update imports of core classes in handlers, messages, and middleware (see the class map above).
6. Check `auto_commit` against the new [commit mode](#offset-commit-mode).
7. Rename Artisan commands in Supervisor configs, schedules, and scripts.
8. Run `php artisan kafka:consumer:route:list` and `php artisan kafka:worker:list` to check the result.
