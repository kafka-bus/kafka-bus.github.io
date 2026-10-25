---
title: Artisan commands
description: Reference for the Kafka Bus Artisan commands — running workers, listing routes, managing offsets and commit records
---

# Commands

The Laravel bridge registers Artisan commands for running workers, inspecting routes, managing consumer group offsets, and maintaining the Commiter store. Commands are registered only when the application runs in the console.

## Command overview

| Command | Description |
|---|---|
| `kafka:consume {workerName*}` | Runs one or several workers in one process |
| `kafka:consume:routes {--expect=*}` | Runs every consumer route in one process |
| `kafka:worker:list` | Lists workers and their topics |
| `kafka:consumer:route:list` | Lists consumer routes: topic → handler and middleware |
| `kafka:publisher:route:list` | Lists producer routes: message class → topic |
| `kafka:consumer:offset:show {topicKey?}` | Shows consumer group offsets |
| `kafka:consumer:offset:set {topicKey} {offset} {--partition=}` | Sets consumer group offsets |
| `kafka:commit {key}` | Creates a commit record manually |
| `kafka:commit:purge {--days=}` | Deletes stale commit records |

::: info Changed in 2.0
`kafka:consume:all` → `kafka:consume:routes`, `kafka:route:list` → `kafka:publisher:route:list`, `kafka:offset:*` → `kafka:consumer:offset:*` without the worker argument. `kafka:consumer:route:list` is new. See [Upgrading from 1.x](/docs/2.x/upgrade#artisan-commands).
:::

## kafka:consume

Runs the blocking read loop for the given workers. The process keeps running until it receives `SIGTERM`, `SIGINT`, or `SIGQUIT`.

```bash
php artisan kafka:consume products
php artisan kafka:consume default
php artisan kafka:consume products orders   # several workers in one process
```

A worker name is a key from `consumers.workers` or a topic key from `consumers.routes`. With several names, their topics are merged into one consumer, and the options of the first worker are used.

On a signal, the command calls `forceStop()`: the current message is finished and committed, then the process exits.

Exit codes:

| Code | Situation |
|---|---|
| `0` | The loop finished cleanly (`Consumer finished`) |
| `1` | No worker name was given, or a `ConsumerException` occurred |

On a `ConsumerException`, the message is written both to `LoggerInterface::error` and to the command output.

::: warning
The command does not catch `MessageConsumerNotHandledException` from handlers — it propagates and stops the process. This is intentional: when Supervisor restarts the worker, it continues from the unprocessed message.
:::

::: tip
In production, run the command under Supervisor — see [the config](/docs/2.x/laravel/consumers#supervisor-configuration).
:::

## kafka:consume:routes

Runs every topic from `consumers.routes` in one process, with the global consumer options. Configured workers are ignored.

```bash
php artisan kafka:consume:routes

# Exclude topics
php artisan kafka:consume:routes --expect=orders --expect=legacy
```

`--expect` takes topic keys. If nothing is left to read, the command prints `No workers for consuming.` and exits with code `1`.

## kafka:worker:list

Prints every worker — declared in `consumers.workers` or derived from a route — with its topics and handlers.

```bash
php artisan kafka:worker:list
```

```
+----------+-----------+----------------------------+--------------------------------------+
| Worker   | Topic key | Topic name                 | Handler                              |
+----------+-----------+----------------------------+--------------------------------------+
| default  | products  | production.fact.products.1 | App\Kafka\Consumers\ProductsConsumer |
| default  | orders    | production.fact.orders.1   | App\Kafka\Consumers\OrdersConsumer   |
| products | products  | production.fact.products.1 | App\Kafka\Consumers\ProductsConsumer |
| orders   | orders    | production.fact.orders.1   | App\Kafka\Consumers\OrdersConsumer   |
+----------+-----------+----------------------------+--------------------------------------+
```

One row per "worker + topic" pair. The handler is taken from `consumers.routes` — `?` means the topic has no route. If there are no workers, the command prints `No workers registered`.

## kafka:consumer:route:list

Prints the consumer routes as the bus sees them: topic, handler, and the final middleware chain (global + route).

```bash
php artisan kafka:consumer:route:list
```

```
+-----------+----------------------------+--------------------------------------+----------------------------+
| Topic key | Topic name                 | Handler                              | Middleware                 |
+-----------+----------------------------+--------------------------------------+----------------------------+
| products  | production.fact.products.1 | App\Kafka\Consumers\ProductsConsumer | ConsumerCommiterMiddleware |
| orders    | production.fact.orders.1   | App\Kafka\Consumers\OrdersConsumer   | ConsumerCommiterMiddleware |
|           |                            |                                      | TenantMiddleware           |
+-----------+----------------------------+--------------------------------------+----------------------------+
```

The command resolves handlers and middleware through the container, so it also confirms every class exists and can be built. Middleware is shown with full class names.

## kafka:publisher:route:list

Prints the producer routes.

```bash
php artisan kafka:publisher:route:list
```

```
+-----------------------------------+-----------+----------------------------+--------------------------------+
| Message                           | Topic key | Topic name                 | Middleware                     |
+-----------------------------------+-----------+----------------------------+--------------------------------+
| App\Kafka\Messages\ProductMessage | products  | production.fact.products.1 |                                |
| App\Kafka\Messages\OrderMessage   | orders    | production.fact.orders.1   | PublisherIdempotencyMiddleware |
+-----------------------------------+-----------+----------------------------+--------------------------------+
```

## kafka:consumer:offset:show

Shows the current, minimum, and maximum offset of every partition for consumed topics. The consumer group is taken from the global `consumers.additional_options.group.id`.

```bash
# All topics from consumers.routes
php artisan kafka:consumer:offset:show

# One topic
php artisan kafka:consumer:offset:show products
```

```
+-----------+----------------------------+-----------+---------+-----+-----+
| Topic key | Topic name                 | Partition | Current | Min | Max |
+-----------+----------------------------+-----------+---------+-----+-----+
| products  | production.fact.products.1 | 0         | 142     | 0   | 200 |
| products  | production.fact.products.1 | 1         | 90      | 0   | 150 |
+-----------+----------------------------+-----------+---------+-----+-----+
```

Handy for monitoring consumer lag: lag = `Max - Current`.

::: tip
A partition of `n/a` with `-1` values means the topic doesn't exist in the cluster yet. That's normal for a fresh environment.
:::

::: warning
Offsets are shown for the global consumer group. A worker with its own `group.id` in `additional_options` isn't covered by these commands — use [Metadata](/docs/2.x/components/metadata) directly for it.
:::

## kafka:consumer:offset:set

Sets the committed offset of the global consumer group for a topic. Lets you re-read messages or skip a backlog.

```bash
php artisan kafka:consumer:offset:set {topicKey} {offset} {--partition=}
```

The `offset` argument accepts:

| Value | Action |
|---|---|
| `earliest`, `early`, `beginning` | The lowest available offset |
| `latest`, `end` | The highest available offset |
| An integer ≥ 0 | A specific position |

Case doesn't matter. Any other value prints an error with a hint.

### Re-reading from the beginning

```bash
php artisan kafka:consumer:offset:set products earliest
```

### Skipping the backlog

```bash
php artisan kafka:consumer:offset:set products latest
```

### Setting a specific offset for one partition

```bash
php artisan kafka:consumer:offset:set products 150 --partition=0
```

Without `--partition`, the change applies to every partition of the topic.

The command prints the result:

```
+-----------+----------------------------+-----------+-----+-----+
| Topic key | Topic name                 | Partition | Old | New |
+-----------+----------------------------+-----------+-----+-----+
| products  | production.fact.products.1 | 0         | 142 | 0   |
| products  | production.fact.products.1 | 1         | 90  | 0   |
+-----------+----------------------------+-----------+-----+-----+
```

The command exits with code `1` and a message if:

- the topic key isn't registered (`TopicCannotResolvedException`);
- the topic doesn't exist in the cluster, or the partition doesn't exist;
- the numeric offset is outside `[Min, Max]`.

::: danger Stop the workers first
Stop every worker of the consumer group before changing offsets. A running consumer overwrites the new position with its next commit.
:::

## kafka:commit

Creates a commit record in the Commiter store for a key. Useful when a message needs to be marked as processed without running its handler.

```bash
php artisan kafka:commit order-12345
```

If a commit already exists for the key, the command prints a warning and exits with code `1`:

```
 WARN  Commit for key [order-12345] already exists.
```

Otherwise, the commit is saved:

```
 INFO  Commit for key [order-12345] has been saved.
```

::: warning The key must match what the consumer computes
The command works with a raw key. With `IdempotencyMessageRepository`, the real key looks like `"{x-idempotency-key}-{full topic name}"`, for example `order-12345-production.fact.orders.1`; without the header — `"{partition}-{offset}-{topic name}"`.

The exact key is in the logs of `ConsumerCommiterMiddleware`, in the `msg_id` field.
:::

## kafka:commit:purge

Deletes commit records older than a given age — otherwise the table grows forever.

```bash
php artisan kafka:commit:purge
php artisan kafka:commit:purge --days=30
```

Without `--days`, the value comes from `kafka-bus-commiter.purge.days` (`14` by default).

```
 INFO  Purged 1523 commit(s) older than 14 day(s).
```

::: warning Database driver only
The command requires the active source to implement `PurgeInterface`. For the `redis` source it exits with code `1` and the message `The active repository source does not support purging` — keys expire by TTL instead.
:::

Schedule it to run regularly:

```php
// routes/console.php
Schedule::command('kafka:commit:purge')->daily();
```
