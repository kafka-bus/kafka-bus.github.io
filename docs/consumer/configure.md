# Configure Consumer

## Processing Flow

```
Application
    │
    ▼ listener(workerName)
  Bus → Thread → ListenerFactory → WorkerRegistry
    │
    ▼ listen()
  Listener → ConsumerStream (blocking loop)
    │
    ▼ getMessage()
  Consumer → Kafka Broker
    │ ← ConsumerMessage
    ▼
  MessageHandler
    │ → Consumer Pipeline (worker middleware)
    ▼
  ConsumerRouter → RouteExecutor
    │ → MessageFactory (deserialization)
    │ → Route Pipeline (route middleware)
    ▼
  Handler (business logic)
    │
    ▼ commit()
```

In more detail:

1. The application requests a listener: `Bus::listener('worker-name')`.
2. `ListenerFactory` retrieves the `Worker` from `WorkerRegistryInterface` and assembles a `ConsumerStream`.
3. `ConsumerStream` starts the blocking read loop.
4. Each message passes through the worker's Consumer Pipeline.
5. `ConsumerRouter` finds the route by the **physical topic name**.
6. `MessageFactory` converts the message into the type the handler expects.
7. The message passes through the Route Pipeline and reaches the handler.
8. On success, `commit()` runs.

## Routes

`ConsumerRoutesBuilder` maps a logical topic key to a handler:

```php
use KafkaBus\Core\Consumers\Router\ConsumerRoutesBuilder;
use KafkaBus\Core\Consumers\Router\RouteInfo;

$consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', new ProductHandler()))
    ->add(new RouteInfo('orders',   new OrderHandler()))
    ->build();
```

### Route Parameters

```php
new RouteInfo(
    topicKey:   'orders',            // logical topic key
    handler:    new OrderHandler(),  // any callable
    middleware: [],                  // list<MessagePipelineMiddleware>
);
```

| Parameter    | Type                                | Default | Description                     |
|---------------|----------------------------------------|-----------|------------------------------------|
| `topicKey`   | `string`                            | —         | Topic key from `TopicRegistry`     |
| `handler`    | `callable`                          | —         | The message handler                |
| `middleware` | `list<MessagePipelineMiddleware>`   | `[]`      | Route-level middleware              |

### Route with Middleware

```php
$consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry)
    ->add(new RouteInfo(
        topicKey:   'orders',
        handler:    new OrderHandler(),
        middleware: [new TenantMiddleware()],
    ))
    ->build();
```

### Default Message Factory

The second argument to `ConsumerRoutesBuilder::make()` sets a factory that applies to any route where the handler's argument type couldn't be detected:

```php
use KafkaBus\Core\Consumers\Messages\StringMessageFactory;

$consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry, new StringMessageFactory())
    ->add(new RouteInfo('products', $legacyHandler))
    ->build();
```

Without a second argument, `NativeMessageFactory` is used. Resolution priority is described in [Consuming Messages](/docs/consumer/consuming#factory-resolution-order).

## Workers

A worker is a named consumer configuration. A single worker can listen to multiple topics:

```php
use KafkaBus\Core\Bus\Listeners\Workers\MemoryWorkerRegistry;
use KafkaBus\Core\Bus\Listeners\Workers\Options;
use KafkaBus\Core\Bus\Listeners\Workers\Worker;

$workerRegistry = MemoryWorkerRegistry::make()
    ->add(new Worker(
        name:    'default',
        routes:  $consumerRoutes,
        options: new Options(
            additionalOptions: [
                'group.id'              => 'my-service',
                'auto.offset.reset'     => 'earliest',
                'max.poll.interval.ms'  => 300_000,
                'session.timeout.ms'    => 45_000,
                'heartbeat.interval.ms' => 3_000,
            ],
            middleware:      [],
            autoCommit:      false,
            consumerTimeout: 5_000,
        )
    ));
```

The registry is passed into `ListenerFactory`:

```php
use KafkaBus\Core\Bus\Listeners\ListenerFactory;
use KafkaBus\Core\Consumers\ConsumerStreamFactory;

$listenerFactory = new ListenerFactory(
    new ConsumerStreamFactory(),
    $workerRegistry,
);
```

Looking up an unregistered worker name throws a `ListenerException`.

### Worker Options

| Parameter            | Type                                | Default | Description                                          |
|-------------------------|----------------------------------------|-----------|------------------------------------------------------------|
| `additionalOptions` | `array<string, scalar\|null>`      | `[]`      | librdkafka parameters for the consumer                    |
| `middleware`        | `list<ConsumerPipelineMiddleware>` | `[]`      | Worker-level middleware (all topics)                        |
| `autoCommit`        | `bool`                              | `true`    | Selects the offset commit implementation                    |
| `consumerTimeout`   | `int`                                | `2000`    | Timeout for waiting on a message in `consume()`, ms         |

::: warning Filtering additionalOptions
Keys are filtered through the `Options::CONSUMER_OPTIONS` allow-list — producer parameters (e.g. `compression.codec`) are silently dropped. Full list — in [Configuration](/docs/configuration#filtering-producer-and-consumer-parameters).
:::

## Committing Offsets

The `autoCommit` flag selects which `CommiterInterface` implementation `Consumer::commit()` uses:

| `autoCommit` | Class             | Behavior                                                        |
|---------------|--------------------|----------------------------------------------------------------|
| `true`       | `DefaultCommiter` | Calls `KafkaConsumer::commitAsync()` after every message          |
| `false`      | `VoidCommiter`    | Does nothing — the commit is left to librdkafka                   |

::: tip How this relates to `enable.auto.commit`
These are two independent mechanisms. `autoCommit` controls the explicit commit issued from kafka-bus code, while `enable.auto.commit` controls periodic background commits inside librdkafka.

Typical combinations:

- **`autoCommit: true` + `enable.auto.commit: 'false'`** — commit strictly after each message is successfully processed. Predictable, at-least-once, recommended by default.
- **`autoCommit: false` + `enable.auto.commit: 'true'`** — the offset advances on a timer regardless of processing outcome. Faster, but a process crash loses some messages.
:::

The commit runs through a `RetryRepeater`: up to 10 attempts with a 1000 µs pause.

## Loop Resilience

`ConsumerStream` ignores routine "empty" broker responses and keeps running:

| Error code                             | Cause                              |
|-------------------------------------------|----------------------------------------|
| `RD_KAFKA_RESP_ERR__PARTITION_EOF`    | End of partition reached                |
| `RD_KAFKA_RESP_ERR__TRANSPORT`        | Transient network error                 |
| `RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT` | Broker didn't respond in time            |
| `RD_KAFKA_RESP_ERR__TIMED_OUT`        | `consumerTimeout` expired               |

Any other code is rethrown as a `MessageConsumerException` and stops the worker.

::: tip
`RD_KAFKA_RESP_ERR__TIMED_OUT` happens routinely every `consumerTimeout` ms when there are no new messages on the topic. It's not an error — the loop just moves to its next iteration.
:::

## Consumer Options

| librdkafka parameter     | Description                       | Recommended value        |
|-----------------------------|--------------------------------------|-------------------------------|
| `group.id`              | Consumer group name                  | Your service name              |
| `auto.offset.reset`     | Starting position on first run       | `earliest` or `latest`         |
| `max.poll.interval.ms`  | Max time between polls               | `300000`                        |
| `session.timeout.ms`    | Session timeout                      | `45000`                         |
| `heartbeat.interval.ms` | Heartbeat interval                   | `3000`                          |
| `enable.auto.commit`    | Background offset auto-commit        | `'false'` (as a string)         |

::: warning
Pass `enable.auto.commit` as the string `'false'`, not `false`. A boolean is correctly converted to `'false'`, but a numeric `0` or `null` would be dropped by the filtering — more details in [Value Normalization](/docs/configuration#value-normalization).
:::

## Viewing and Resetting Offsets

`Listener::partitions()` gives access to the worker's current offsets and lets you reset them:

```php
use KafkaBus\Core\Bus\Listeners\Partitions\CommitOffset;
use KafkaBus\Core\Bus\Listeners\Partitions\Offset;

$partitions = $bus->listener('products')->partitions();

foreach ($partitions->list() as $partition) {
    echo "{$partition->topic->key} #{$partition->id}: "
       . "{$partition->currentOffset} / [{$partition->minOffset}, {$partition->maxOffset}]\n";
}

// Re-read the topic from the beginning across all partitions
$partitions->setOffset(new CommitOffset(
    topic:     $topicRegistry->get('products'),
    partition: RD_KAFKA_PARTITION_UA, // all partitions
    offset:    Offset::Early,         // or Offset::Latest, or a specific number
));
```

`setOffset()` throws a `CannotCommitOffsetException` if the topic doesn't belong to the worker, the partition doesn't exist, or the offset is outside `[minOffset, maxOffset]`.

::: warning
`partitions()` is only available for connections implementing `ConnectionHasTopicsInterface` (i.e. `KafkaConnection`). For `NullConnection` it throws a `LogicException`.

The worker must be stopped: a running process will overwrite the position on its next commit.
:::

In Laravel these operations are available as the `kafka:offset:show` and `kafka:offset:set` commands — see [Commands](/docs/laravel/commands).
