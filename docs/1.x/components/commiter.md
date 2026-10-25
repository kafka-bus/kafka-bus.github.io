# Commiter

A middleware package for [kafka-bus](https://github.com/kafka-bus/kafka-bus) that provides idempotent message processing: tracks processed messages, prevents duplicates, and limits the number of processing attempts.

## Installation

```bash
composer require kafka-bus/commiter
```

## Architecture

The package is split into two layers, and understanding that split matters when configuring it:

```
ConsumerCommiterMiddleware
        │ works with
        ▼
ConsumerMessageRepositoryInterface   ← how to compute a key from the message
        │  (IdempotencyMessageRepository | NativeMessageRepository)
        │ delegates storage to
        ▼
RepositorySourceInterface            ← where state is physically stored
   (ArrayRepositorySource | DatabaseRepositorySource | RedisRepositorySource)
```

| Layer                                 | Responsible for                                            |
|------------------------------------------|------------------------------------------------------------|
| `ConsumerMessageRepositoryInterface` | How to derive a deduplication key from a Kafka message         |
| `RepositorySourceInterface`          | How to store and update state for that key                      |

### Attempt

The state for a single key is described by an immutable object:

```php
namespace KafkaBus\Commiter;

final readonly class Attempt
{
    public function __construct(
        public string $key,
        public int $number = 1,
        public ?DateTimeImmutable $commitedAt = null,
    ) {}
}
```

| Field        | Description                                                          |
|---------------|------------------------------------------------------------------------|
| `key`        | The deduplication key                                                  |
| `number`     | The current attempt number                                             |
| `commitedAt` | The time of successful processing; `null` means not yet processed       |

## How It Works

`ConsumerCommiterMiddleware` plugs into the consumer pipeline and handles four scenarios:

| Scenario                                          | Action                                                  |
|-------------------------------------------------------|----------------------------------------------------------|
| Message already processed (`commitedAt != null`)  | Logs a `warning`, stops the pipeline                        |
| `maxAttempt` exceeded                             | Logs an `error`, stops the pipeline                          |
| Message processed successfully                    | Calls `commit()`, logs a `debug`                             |
| Handler threw an exception                         | Calls `failed()`, rethrows the exception                     |

In the first two cases, the pipeline returns **without** calling `continue()` — the handler never runs, but the offset is still committed and the message won't come back.

Every log entry includes context: `msg_id`, `topic_name`, `partition`, `offset`.

## Middleware Parameters

```php
new ConsumerCommiterMiddleware(
    repository: $repository,       // ConsumerMessageRepositoryInterface
    logger:     new NullLogger(),  // PSR-3
    maxAttempt: 3,                 // -1 = unlimited
);
```

| Parameter     | Type                                  | Default        | Description                                    |
|-----------------|------------------------------------------|-------------------|------------------------------------------------------|
| `repository` | `ConsumerMessageRepositoryInterface`  | —                 | The key-derivation and storage strategy               |
| `logger`     | `LoggerInterface`                      | `NullLogger`      | A PSR-3 compatible logger                              |
| `maxAttempt` | `int`                                   | `-1`              | Max attempts; `-1` or `0` means unlimited              |

::: warning
The first argument is a `ConsumerMessageRepositoryInterface` (`IdempotencyMessageRepository` or `NativeMessageRepository`), **not** a `RepositorySourceInterface`. The storage source is passed into the repository itself.
:::

## Choosing a Repository

### By Idempotency Header (Recommended)

Reads the `x-idempotency-key` header and builds the key as `"{header}-{topicName}"`. The same idempotency key on different topics is treated as different events. When the header is missing, `msgId()` is used instead, so older producers without idempotency support keep working.

```php
use KafkaBus\Commiter\Repositories\IdempotencyMessageRepository;

$repository = new IdempotencyMessageRepository($source);
```

### NativeMessageRepository

Uses `msgId()` — a string like `"{partition}-{offset}-{topicName}"`.

## Choosing a Storage Source

| Source                       | Package               | Notes                                                |
|--------------------------------|--------------------------|------------------------------------------------------|
| `ArrayRepositorySource`    | `kafka-bus/commiter`   | In-process memory; for tests                            |
| `DatabaseRepositorySource` | `laravel-bridge`       | The `kafka_bus_commits` table, supports pruning          |
| `RedisRepositorySource`    | `laravel-bridge`       | TTL hashes, atomic Lua scripts                           |

::: tip
Outside Laravel, implement `RepositorySourceInterface` for your own storage — it's just three methods.
:::

## Basic Usage

### 1. Implement a Storage Source

```php
use DateTimeImmutable;
use KafkaBus\Commiter\Attempt;
use KafkaBus\Commiter\Interfaces\RepositorySourceInterface;

final class PdoRepositorySource implements RepositorySourceInterface
{
    public function __construct(private readonly PDO $pdo) {}

    /**
     * Returns the current state for a key.
     * null means the message hasn't been seen before.
     */
    public function get(string $key): ?Attempt
    {
        $stmt = $this->pdo->prepare('SELECT * FROM kafka_bus_commits WHERE `key` = ?');
        $stmt->execute([$key]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        if ($row === false) {
            return null;
        }

        return new Attempt(
            key: $row['key'],
            number: (int) $row['number'],
            commitedAt: $row['commited_at'] !== null
                ? new DateTimeImmutable($row['commited_at'])
                : null,
        );
    }

    /**
     * Increments the failed-attempt counter without marking the key as processed.
     */
    public function increment(string $key): void
    {
        $this->pdo->prepare(
            'INSERT INTO kafka_bus_commits (`key`, number, commited_at) VALUES (?, 1, NULL)
             ON DUPLICATE KEY UPDATE number = number + 1'
        )->execute([$key]);
    }

    /**
     * Marks the key as successfully processed.
     */
    public function commit(string $key): void
    {
        $now = (new DateTimeImmutable())->format('Y-m-d H:i:s');

        $this->pdo->prepare(
            'INSERT INTO kafka_bus_commits (`key`, number, commited_at) VALUES (?, 1, ?)
             ON DUPLICATE KEY UPDATE number = number + 1, commited_at = VALUES(commited_at)'
        )->execute([$key, $now]);
    }
}
```

The contract for the three methods:

| Method              | Called                                     | Must                                                    |
|-----------------------|-----------------------------------------------|------------------------------------------------------------|
| `get()`            | Before processing                              | Return `null` if the key doesn't exist                        |
| `increment()`      | After an exception in the handler              | Increment `number`, leave `commitedAt` empty                   |
| `commit()`         | After successful processing                    | Set `commitedAt`                                                |

::: warning
Implement `increment()` and `commit()` atomically (`upsert`, `INSERT ... ON DUPLICATE KEY UPDATE`, a Lua script). Multiple consumers in the same group can process messages in parallel, and a "read, then write" approach will race.
:::

### 2. Attach the Middleware

```php
use KafkaBus\Commiter\Middleware\ConsumerCommiterMiddleware;
use KafkaBus\Commiter\Repositories\IdempotencyMessageRepository;
use KafkaBus\Core\Bus\Listeners\Workers\MemoryWorkerRegistry;
use KafkaBus\Core\Bus\Listeners\Workers\Options;
use KafkaBus\Core\Bus\Listeners\Workers\Worker;

$repository = new IdempotencyMessageRepository(new PdoRepositorySource($pdo));

$workerRegistry = MemoryWorkerRegistry::make()
    ->add(new Worker(
        name:    'default',
        routes:  $consumerRoutes,
        options: new Options(
            middleware: [
                new ConsumerCommiterMiddleware(
                    repository: $repository,
                    logger:     $logger,
                    maxAttempt: 3,
                )
            ]
        )
    ));
```

::: tip Middleware order
Put `ConsumerCommiterMiddleware` **first** in the list. That way duplicate checking happens before logging, metrics, and other wrapping middleware run.
:::

### Example for Tests

```php
use KafkaBus\Commiter\Repositories\ArrayRepositorySource;

$source = new ArrayRepositorySource();
$repository = new IdempotencyMessageRepository($source);

// after running the pipeline
assert($source->get('order-42-fact.orders.1')?->commitedAt !== null);
```

State only lives for the duration of the process — not suitable for production.

## Idempotency Keys

By default, the deduplication key is `msgId()`, a combination of partition, offset, and topic. This works as long as a single physical message doesn't show up again with a different offset.

**The problem:** on redelivery, re-publishing, or cross-cluster mirroring, the same logical event arrives with a different `msgId()` and bypasses the check.

**The solution:** an idempotency key is a stable identifier that the producer puts in the `x-idempotency-key` header. The consumer uses it instead of `msgId()`.

### Choosing the Right Key

✅ Good choices:
- `order-42-v3` (aggregate id + version)
- an outbox table row id
- any value the upstream system considers unique for the event

❌ Bad choices:
- a timestamp (changes on redelivery)
- a random UUID generated on every send

## ProducerIdempotencyMiddleware

### Add Idempotency to a Message

```php
use KafkaBus\Commiter\Interfaces\HasIdempotency;
use KafkaBus\Core\Interfaces\Producers\Messages\ProducerMessageInterface;

final readonly class ProductCreatedMessage implements ProducerMessageInterface, HasIdempotency
{
    public function __construct(
        private string $productId,
        private string $payload,
    ) {}

    public function toPayload(): string
    {
        return $this->payload;
    }

    public function getIdempotencyKey(): string
    {
        return $this->productId; // stable key
    }
}
```

### Attach the Middleware to a Producer Route

```php
use KafkaBus\Commiter\Middleware\ProducerIdempotencyMiddleware;
use KafkaBus\Core\Bus\Publishers\Router\Options;
use KafkaBus\Core\Bus\Publishers\Router\PublisherRoutesBuilder;

$publisherRoutes = PublisherRoutesBuilder::make($topicRegistry)
    ->add(
        messageClass: ProductCreatedMessage::class,
        topicKey:     'products',
        options:      new Options(
            middleware: [new ProducerIdempotencyMiddleware()]
        )
    )
    ->build();
```

The middleware adds an `x-idempotency-key` header to every outgoing message. If the message doesn't implement `HasIdempotency`, no header is added and the message publishes as usual.

## Limiting Attempts

`maxAttempt` protects against "poison" messages that fail on every processing attempt:

```php
new ConsumerCommiterMiddleware($repository, $logger, maxAttempt: 3);
```

How it works:

1. The handler fails → `failed()` increments `number`, the exception propagates and stops the worker;
2. The worker restarts (Supervisor), and the message is read again from the same offset;
3. Once `number` exceeds `maxAttempt`, the middleware logs an `error` and skips the message without calling the handler;
4. The offset is committed and the worker moves on.

::: warning
`maxAttempt` doesn't make the worker fault-tolerant on its own: the process still crashes between attempts. To handle errors without a restart, add middleware with `try/catch` — see [Consumer Pipeline](/docs/1.x/consumer/pipeline#middleware-with-error-handling). But then `failed()` never runs, and the attempt counter stops growing.
:::

## Laravel Integration

`laravel-bridge` already wires up this entire layer: sources (`database`, `redis`), repository selection, the table migration, and the `kafka:commit` / `kafka:commit:purge` commands. See [Laravel Configuration](/docs/1.x/laravel/configuration#commiter).
