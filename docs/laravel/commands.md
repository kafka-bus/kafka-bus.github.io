# Commands

## Command Overview

| Command                                        | Description                                              |
|--------------------------------------------------|---------------------------------------------------------|
| `kafka:consume {worker*}`                     | Starts a long-running consumer process                     |
| `kafka:consume:all {--expect=*}`             | Starts every worker in a single process                    |
| `kafka:worker:list`                          | Lists workers, topics, and handlers                         |
| `kafka:route:list`                           | Lists producer routes                                        |
| `kafka:offset:show {worker}`                 | Shows a worker's current offsets                             |
| `kafka:offset:set {worker} {topic} {offset}` | Sets an offset                                                |
| `kafka:commit {key}`                         | Manually creates a commit record                             |
| `kafka:commit:purge {--days=}`               | Deletes stale commit records                                  |

Commands are only registered when running in the console.

## kafka:consume

Starts the blocking read loop for the given workers. The process keeps running until it receives `SIGTERM`, `SIGINT`, or `SIGQUIT`.

```bash
php artisan kafka:consume products
php artisan kafka:consume default
php artisan kafka:consume products orders   # multiple workers in one process
```

The command implements `SignalableCommandInterface`: on a signal, `forceStop()` is called, the current message finishes processing and gets committed, and only then does the process exit cleanly.

Exit codes:

| Code | Situation                                                            |
|-------|--------------------------------------------------------------------|
| `0` | The loop finished cleanly (`Consumer finished`)                        |
| `1` | No worker was passed, or a `ConsumerException` occurred                |

On error, the message is written both to `LoggerInterface::error` and to the command's output.

::: warning
The command does **not** catch `MessageConsumerNotHandledException` exceptions from the handler — they propagate outward and crash the process. This is intentional: a restart via Supervisor brings the consumer back to the unprocessed message.
:::

::: tip
In production, wrap the command in Supervisor — see [the config](/docs/laravel/consumers#supervisor-configuration).
:::

## kafka:consume:all

Starts every worker from `kafka-bus.consumers.workers` in a single process.

```bash
php artisan kafka:consume:all

# Exclude specific workers
php artisan kafka:consume:all --expect=products-analytics --expect=legacy
```

::: warning
All workers are merged into a single consumer, and connection settings are taken from the first one. If workers have different `group.id`s or commit modes, only the first worker's configuration applies — run such workers as separate processes instead.
:::

## kafka:worker:list

Prints the full list of registered workers along with their configuration.

```bash
php artisan kafka:worker:list
```

```
+----------+-----------+----------------------------+------------------------------------------+---------------------+------------------+
| Worker   | Topic key | Topic name                 | Handler                                  | Consumer Middleware | Route Middleware |
+----------+-----------+----------------------------+------------------------------------------+---------------------+------------------+
| default  | products  | production.fact.products.1 | App\Kafka\Consumers\ProductsConsumer     |                     |                  |
| default  | orders    | production.fact.orders.1   | App\Kafka\Consumers\OrdersConsumer       | ConsumerCommiter... | TenantMiddleware |
| products | products  | production.fact.products.1 | App\Kafka\Consumers\ProductsConsumer     |                     |                  |
+----------+-----------+----------------------------+------------------------------------------+---------------------+------------------+
```

One row per "worker + topic" pair. The command resolves handlers and middleware through the container, so it also confirms every class exists and can be instantiated. If there are no workers, it prints a `No workers registered` warning.

## kafka:route:list

Prints the registered producer routes.

```bash
php artisan kafka:route:list
```

```
+------------------------------------------+-----------+----------------------------+---------------------------------+
| Message                                  | Topic key | Topic name                 | Middleware                      |
+------------------------------------------+-----------+----------------------------+---------------------------------+
| App\Kafka\Messages\ProductMessage         | products  | production.fact.products.1 |                                 |
| App\Kafka\Messages\OrderMessage           | orders    | production.fact.orders.1   | ProducerIdempotencyMiddleware   |
+------------------------------------------+-----------+----------------------------+---------------------------------+
```

## kafka:offset:show

Shows the current, minimum, and maximum offset for every partition of every topic a worker listens to.

```bash
php artisan kafka:offset:show products
```

```
+-----------+----------------------------+-----------+---------+-----+-----+
| Topic key | Topic name                 | Partition | Current | Min | Max |
+-----------+----------------------------+-----------+---------+-----+-----+
| products  | production.fact.products.1 | 0         | 142     | 0   | 200 |
| products  | production.fact.products.1 | 1         | 90      | 0   | 150 |
+-----------+----------------------------+-----------+---------+-----+-----+
```

Handy for monitoring consumer lag. Lag = `Max - Current`.

::: tip
A partition of `n/a` with `-1` values means the topic hasn't been created on the cluster yet, or has no metadata. That's normal for a fresh environment.
:::

## kafka:offset:set

Sets the committed offset for a worker's topic. Lets you re-read messages or skip an accumulated backlog.

```bash
kafka:offset:set {workerName} {topicKey} {offset} {--partition=}
```

The `offset` argument accepts:

| Value                              | Action                             |
|----------------------------------------|-----------------------------------|
| `earliest`, `early`, `beginning`  | The lowest available offset           |
| `latest`, `end`                   | The highest available offset          |
| An integer ≥ 0                     | A specific position                    |

Case doesn't matter. Any other value raises an error with a hint.

### Re-reading from the Beginning

```bash
php artisan kafka:offset:set products products earliest
```

### Jumping to the Latest Message (Skipping the Backlog)

```bash
php artisan kafka:offset:set products products latest
```

### Setting a Specific Offset

```bash
php artisan kafka:offset:set products products 150
```

### Setting an Offset for a Specific Partition

```bash
php artisan kafka:offset:set products products 150 --partition=0
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

The command exits with code `1` and a clear message if:

- the topic key isn't registered (`TopicCannotResolvedException`);
- the topic doesn't belong to the worker, or the given partition doesn't exist;
- the numeric offset is outside `[Min, Max]`;
- the worker isn't registered (`ListenerException`).

::: danger Important
The worker must be **stopped** while resetting the offset. If the consumer process is still running, it will overwrite the new position on its next commit.
:::

## kafka:commit

Manually creates a commit record in the idempotency store for a given key. Useful when a message needs to be marked as processed without going through the standard consumer pipeline.

```bash
php artisan kafka:commit {key}
```

### Example

```bash
php artisan kafka:commit order-12345
```

If a commit already exists for that key, the command prints a warning and exits with code `1`:

```
 WARN  Commit for key [order-12345] already exists.
```

Otherwise, the commit is saved:

```
 INFO  Commit for key [order-12345] has been saved.
```

::: warning The key must match what the consumer computes
The command works with a raw key. With `IdempotencyMessageRepository` active, the real key looks like `"{x-idempotency-key}-{full topic name}"`, e.g. `order-12345-production.fact.orders.1`; without the header, it looks like `"{partition}-{offset}-{topic name}"`.

You can find the exact key in `ConsumerCommiterMiddleware`'s logs — it's written to the `msg_id` field.
:::

::: tip
This command is meant for recovery scenarios — for example, when a message was processed outside the consumer, or needs to be permanently skipped.
:::

## kafka:commit:purge

Deletes commit records older than a given age — otherwise the table grows forever.

```bash
php artisan kafka:commit:purge
php artisan kafka:commit:purge --days=30
```

Without `--days`, the value comes from `kafka-bus-commiter.purge.days` (defaults to `14`).

```
 INFO  Purged 1523 commit(s) older than 14 day(s).
```

Only records with a non-null `commited_at` are removed — incomplete attempts are kept.

::: warning Database driver only
The command requires the active source to implement `PurgeInterface`. For the `redis` source it exits with code `1` and the message `The active repository source does not support purging` — cleanup there happens via key TTL instead.
:::

It's convenient to schedule this command:

```php
// routes/console.php
Schedule::command('kafka:commit:purge')->daily();
```
