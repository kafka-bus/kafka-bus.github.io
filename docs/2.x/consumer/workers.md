---
title: Workers
description: Reading Kafka with kafka-bus/worker — defining workers, running the polling loop, merging workers, offset commits, and graceful shutdown
---

# Workers

A worker is a long-running process that reads messages from Kafka and hands each of them to `Bus::dispatch()`. It knows only which topics to poll and with which consumer options — handlers, routing, and middleware are configured in the bus. Workers live in the `kafka-bus/worker` package.

```bash
composer require kafka-bus/worker
```

The package is already included in the `kafka-bus/kafka-bus` meta-package.

## Defining a worker

A `Worker` is a name, a list of topics, and consumer options. Register workers in a `MemoryWorkerRegistry`:

```php
use KafkaBus\Worker\Options;
use KafkaBus\Worker\Registry\MemoryWorkerRegistry;
use KafkaBus\Worker\Worker;

$workerRegistry = MemoryWorkerRegistry::make()
    ->add(new Worker(
        name: 'products',
        topics: [$topicRegistry->get('products')],
        options: new Options(
            additionalOptions: [
                'group.id' => 'products-service',
                'auto.offset.reset' => 'earliest',
                'max.poll.interval.ms' => 300_000,
                'session.timeout.ms' => 45_000,
                'heartbeat.interval.ms' => 3_000,
            ],
            autoCommit: true,
            consumerTimeout: 5_000,
        ),
    ));
```

`Worker` parameters:

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `string` | — | Unique worker name in the registry |
| `topics` | `list<Topic>` | — | Topics the worker subscribes to |
| `options` | `Options` | `new Options()` | Consumer options |

`Options` parameters:

| Parameter | Type | Default | Description |
|---|---|---|---|
| `additionalOptions` | `array<string, scalar\|null>` | `[]` | librdkafka consumer parameters |
| `autoCommit` | `bool` | `true` | Offset commit mode, see [Committing offsets](#committing-offsets) |
| `consumerTimeout` | `int` | `2000` | How long `consume()` waits for a message, ms |

::: warning Filtering additionalOptions
Keys are filtered through the `Options::CONSUMER_OPTIONS` allow-list of the connection — producer parameters such as `compression.codec` are silently dropped. Full list — in [Configuration](/docs/2.x/configuration#filtering-producer-and-consumer-parameters).
:::

Every topic a worker polls needs a route in the bus. A message from a topic without a route makes `dispatch()` fail and stops the worker — see [Consumer routes](/docs/2.x/consumer/configure).

## Running a worker

`WorkerRunnerFactory` takes a bus and a worker name and returns a `WorkerRunner`. The runner creates a consumer on `$bus->connection()` and feeds every message into `$bus->dispatch()`:

```php
use KafkaBus\Worker\WorkerRunnerFactory;

$runner = (new WorkerRunnerFactory($workerRegistry))
    ->create($bus, 'products');

$runner->run(); // blocking loop
```

`run()` doesn't return until `forceStop()` is called or an error stops the loop. Each iteration:

1. `Consumer::getMessage()` waits for a message for up to `consumerTimeout` ms;
2. `$bus->dispatch()` runs routing, middleware, and the handler;
3. `Consumer::commit()` confirms the offset — explicitly with `autoCommit: false`, see [Committing offsets](#committing-offsets).

The worker commits only after `dispatch()` succeeds. If it throws a `MessageConsumerNotHandledException`, the exception leaves `run()`, and the worker doesn't commit that message.

::: tip Keeping the worker alive on errors
To keep one bad message from stopping processing, catch the exception in a route middleware — see [Middleware with error handling](/docs/2.x/consumer/pipeline#middleware-with-error-handling).
:::

`WorkerRunnerFactory` constructor parameters:

| Parameter | Type | Default | Description |
|---|---|---|---|
| `workerRegistry` | `WorkerRegistryInterface` | `new MemoryWorkerRegistry()` | Where workers are looked up by name |
| `streamFactory` | `ConsumerStreamFactoryInterface` | `new ConsumerStreamFactory()` | Builds the polling loop |
| `workerMerger` | `WorkerMerger` | `new WorkerMerger()` | Merges several workers into one |

An unknown worker name throws a `WorkerException`.

## Several workers in one process

Pass a list of names to read several workers' topics with one consumer:

```php
$runner = (new WorkerRunnerFactory($workerRegistry))
    ->create($bus, ['products', 'orders']);

$runner->run();
```

`WorkerMerger` combines the workers into a synthetic `grouped-worker`: the topic lists are merged (duplicates removed by topic name), and the options are taken from the first worker. A list with a single name behaves like a plain string.

::: warning
Only the first worker's `additionalOptions`, `autoCommit`, and `consumerTimeout` are used. Merge only workers that share the same `group.id` and commit mode; run the rest as separate processes.
:::

Since handlers live in the bus, merging workers doesn't affect which handler or middleware processes a message — routing is always by topic.

## Committing offsets

The `autoCommit` option chooses who commits the consumer group's offsets — librdkafka in the background, or the worker after each processed message:

| `autoCommit` | `enable.auto.commit` | Commiter | Behavior |
|---|---|---|---|
| `true` (default) | `'true'` | `VoidCommiter` | librdkafka commits offsets in the background every `auto.commit.interval.ms`; the worker doesn't commit itself |
| `false` | `'false'` | `DefaultCommiter` | After each successful `dispatch()`, the worker calls `KafkaConsumer::commitAsync()` for that message |

Which to choose:

- **`autoCommit: false`** — the offset moves only after the handler succeeded. If the process crashes, the unprocessed message is read again (at-least-once). Recommended when losing a message is unacceptable.
- **`autoCommit: true`** — fewer requests to the broker, but the background commit doesn't depend on the processing result: after a crash, a message that was read but not processed can be skipped.

::: warning
`autoCommit` always overwrites `enable.auto.commit` — setting it in `additionalOptions` has no effect.
:::

::: info Fixed in 2.1.1
Before `kafka-bus/core` 2.1.1, the commiters were swapped: `autoCommit: false` committed nothing at all. Update to `^2.1.1` if you rely on explicit commits.
:::

The explicit commit runs through a `RetryRepeater`: up to 10 attempts with a 1000 µs pause.

## Graceful shutdown

```php
pcntl_async_signals(true);

$runner = (new WorkerRunnerFactory($workerRegistry))->create($bus, 'products');

pcntl_signal(SIGINT, fn () => $runner->forceStop());  // Ctrl+C
pcntl_signal(SIGTERM, fn () => $runner->forceStop()); // kill

$runner->run();
```

`forceStop()` sets a flag that's checked at the end of each loop iteration. The process stops only after the current message has been dispatched and its commit has been issued. After the loop ends, the consumer unsubscribes from its topics.

::: tip
In Laravel this is already handled: the `kafka:consume` command handles `SIGTERM`, `SIGINT`, and `SIGQUIT`.
:::

## Loop resilience

The polling loop ignores routine "empty" broker responses and keeps running:

| Error code | Cause |
|---|---|
| `RD_KAFKA_RESP_ERR__PARTITION_EOF` | End of partition reached |
| `RD_KAFKA_RESP_ERR__TRANSPORT` | Transient network error |
| `RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT` | Broker didn't respond in time |
| `RD_KAFKA_RESP_ERR__TIMED_OUT` | `consumerTimeout` expired |

Any other code is rethrown as a `MessageConsumerException` and stops the worker.

::: tip
`RD_KAFKA_RESP_ERR__TIMED_OUT` happens every `consumerTimeout` ms when there are no new messages. It's not an error — the loop just moves to its next iteration.
:::

## Recommended consumer options

| librdkafka parameter | Description | Recommended value |
|---|---|---|
| `group.id` | Consumer group name | Your service name |
| `auto.offset.reset` | Starting position when the group has no offset | `earliest` or `latest` |
| `max.poll.interval.ms` | Max time between polls | `300000` |
| `session.timeout.ms` | Session timeout | `45000` |
| `heartbeat.interval.ms` | Heartbeat interval | `3000` |

## Custom worker registry

`WorkerRunnerFactory` accepts any `WorkerRegistryInterface`. Implement it to load workers from configuration or a database — it has a single method that returns a `Worker` or `null`:

```php
use KafkaBus\Worker\Registry\WorkerRegistryInterface;
use KafkaBus\Worker\Worker;

final class ConfigWorkerRegistry implements WorkerRegistryInterface
{
    public function get(string $workerName): ?Worker
    {
        // ...
    }
}
```

The Laravel bridge uses this to build workers from `config/kafka-bus.php`.
