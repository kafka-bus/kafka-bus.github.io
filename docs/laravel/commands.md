# Artisan Commands

## Command Overview

| Command                                      | Description                                    |
|----------------------------------------------|------------------------------------------------|
| `kafka:consume {worker}`                     | Start a long-running consumer process          |
| `kafka:worker:list`                          | List workers, topics, and handlers             |
| `kafka:route:list`                           | List producer routes                           |
| `kafka:offset:show {worker}`                 | Show current offsets for a worker              |
| `kafka:offset:set {worker} {topic} {offset}` | Set an offset                                  |
| `kafka:commit {key}`                         | Manually create a commit record in the database |

## kafka:consume

Starts a blocking read loop for the specified worker. The process continues until it receives `SIGINT` or `SIGTERM`.

```bash
php artisan kafka:consume products
php artisan kafka:consume default
php artisan kafka:consume products-secondary
```

::: tip
For production, wrap the command in Supervisor to ensure automatic restarts on failure.
:::

## kafka:worker:list

Prints the full list of registered workers with their configuration.

```bash
php artisan kafka:worker:list
```

```
+----------+-----------+----------------------------+------------------------------------------+---------------------+------------------+
| Worker   | Topic key | Topic name                 | Handler                                  | Consumer Middleware | Route Middleware  |
+----------+-----------+----------------------------+------------------------------------------+---------------------+------------------+
| default  | products  | production.fact.products.1 | App\Kafka\Consumers\ProductsConsumer     |                     |                  |
| default  | orders    | production.fact.orders.1   | App\Kafka\Consumers\OrdersConsumer       | ConsumerCommiter... | TenantMiddleware |
| products | products  | production.fact.products.1 | App\Kafka\Consumers\ProductsConsumer     |                     |                  |
+----------+-----------+----------------------------+------------------------------------------+---------------------+------------------+
```

Use this for a quick sanity check that all workers are configured correctly after changing the config.

## kafka:route:list

Prints the registered producer routes.

```bash
php artisan kafka:route:list
```

```
+------------------------------------------+-----------+----------------------------+------------+
| Message                                  | Topic key | Topic name                 | Middleware |
+------------------------------------------+-----------+----------------------------+------------+
| App\Kafka\Messages\ProductMessage        | products  | production.fact.products.1 | 0          |
| App\Kafka\Messages\OrderMessage          | orders    | production.fact.orders.1   | 1          |
+------------------------------------------+-----------+----------------------------+------------+
```

## kafka:offset:show

Shows the current, minimum, and maximum offsets for each partition of each topic in the worker.

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

Useful for monitoring consumer lag. Lag = `Max - Current`.

## kafka:offset:set

Sets the committed offset for a worker's topic. Allows you to re-read messages or skip an accumulated backlog.

```bash
kafka:offset:set {workerName} {topicKey} {offset} {--partition=}
```

### Go to the first message (re-read everything)

```bash
php artisan kafka:offset:set products products earliest
```

### Go to the last message (skip the backlog)

```bash
php artisan kafka:offset:set products products latest
```

### Set a specific offset

```bash
php artisan kafka:offset:set products products 150
```

### Set an offset for a specific partition

```bash
php artisan kafka:offset:set products products 150 --partition=0
```

The command prints the result:

```
+-----------+----------------------------+-----------+-----+-----+
| Topic key | Topic name                 | Partition | Old | New |
+-----------+----------------------------+-----------+-----+-----+
| products  | production.fact.products.1 | 0         | 142 | 0   |
| products  | production.fact.products.1 | 1         | 90  | 0   |
+-----------+----------------------------+-----------+-----+-----+
```

::: danger Important
The worker must be **stopped** while resetting offsets. If the consumer process is running, it will overwrite the new position on the next commit.
:::

## kafka:commit

Manually creates a commit record in the database for the given key. Useful when a message needs to be marked as processed without going through the normal consumer pipeline.

```bash
php artisan kafka:commit {key}
```

### Example

```bash
php artisan kafka:commit order-12345
```

If the key has already been committed, the command outputs a warning and exits with a non-zero code:

```
 WARN  Commit for key [order-12345] already exists.
```

Otherwise, the commit is saved and the command reports success:

```
 INFO  Commit for key [order-12345] has been saved.
```

::: tip
This command is intended for recovery scenarios — for example, when a message was processed outside the consumer or needs to be skipped permanently.
:::
