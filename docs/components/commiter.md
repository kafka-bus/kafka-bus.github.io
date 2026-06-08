# Commiter

A middleware package for [kafka-bus](https://github.com/kafka-bus/kafka-bus) that provides idempotent message processing: tracks processed messages, prevents duplicates, and limits the number of processing attempts.

## Installation

```bash
composer require kafka-bus/commiter
```

## How It Works

`ConsumerCommiterMiddleware` is embedded in the consumer pipeline and handles four scenarios:

| Scenario                                          | Action                                          |
|---------------------------------------------------|-------------------------------------------------|
| Message already processed (`commitedAt != null`)  | Logs a warning, stops the pipeline              |
| `maxAttempt` exceeded                             | Logs an error, stops the pipeline               |
| Message processed successfully                    | Calls `commit()`, marks as processed            |
| Handler threw an exception                        | Calls `failed()`, rethrows the exception        |

## Basic Usage

### 1. Implement the Repository

Implement `RepositorySourceInterface` to store message state (DB, Redis, etc.):

```php
use KafkaBus\Commiter\Attempt;
use KafkaBus\Commiter\Interfaces\RepositorySourceInterface;

class DatabaseRepositorySource implements RepositorySourceInterface
{
    /**
     * Returns the current state of the key.
     * Null means the message has not been seen before.
     */
    public function get(string $key): ?Attempt
    {
        $record = DB::table('kafka_commits')->where('key', $key)->first();

        if (!$record) {
            return null;
        }

        return new Attempt(
            attempts: $record->attempts + 1,
            committedAt: $record->committed_at ? new DateTime($record->committed_at) : null,
        );
    }

    /**
     * Increments the failed attempt counter.
     */
    public function increment(string $key): void
    {
        DB::table('kafka_commits')->upsert(
            ['key' => $key, 'attempts' => 1],
            ['key'],
            ['attempts' => DB::raw('attempts + 1')]
        );
    }

    /**
     * Marks the key as successfully processed.
     */
    public function commit(string $key): void
    {
        DB::table('kafka_commits')->upsert(
            ['key' => $key, 'committed_at' => now(), 'attempts' => 1],
            ['key'],
            ['committed_at' => now()]
        );
    }
}
```

### 2. Attach the Middleware

```php
use KafkaBus\Commiter\Middleware\ConsumerCommiterMiddleware;
use KafkaBus\Commiter\Repositories\NativeMessageRepository;

$repository = new NativeMessageRepository(new DatabaseRepositorySource());

$workerRegistry = MemoryWorkerRegistry::make()
    ->add(new Worker(
        name:    'default',
        routes:  $consumerRoutes,
        options: new Options(
            middleware: [
                new ConsumerCommiterMiddleware(
                    repository: $repository,
                    logger:     new NullLogger(),  // PSR-3
                    maxAttempt: 3,           // -1 = unlimited
                )
            ]
        )
    ));
```

### Middleware Parameters

| Parameter    | Type                        | Default      | Description                              |
|--------------|-----------------------------|--------------|------------------------------------------|
| `repository` | `RepositorySourceInterface` | —            | State storage                            |
| `logger`     | `LoggerInterface`           | `NullLogger` | PSR-3 compatible logger                  |
| `maxAttempt` | `int`                       | `-1`         | Max attempts. `-1` — unlimited           |

## Idempotency Keys

By default, the deduplication key is Kafka's `msgId()` — a combination of topic, partition, and offset. This works as long as the same physical message never appears with a different offset.

**Problem:** with retries, re-sends, or cross-cluster mirroring, the same logical event may arrive with a different `msgId()` and bypass the check.

**Solution:** an idempotency key — a stable identifier that the producer adds to the `x-idempotency-key` header. The consumer uses it instead of `msgId()`.

### Choosing a Good Key

✅ Good options:
- `order-42-v3` (aggregate id + version)
- an outbox table row id
- any value the upstream considers unique for the event

❌ Bad options:
- timestamp (changes on retry)
- a random UUID generated on every send

## ProducerIdempotencyMiddleware

### Implement HasIdempotency on the Message

```php
use KafkaBus\Core\Interfaces\Producers\Messages\ProducerMessageInterface;
use KafkaBus\Commiter\Interfaces\HasIdempotency;

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

### Attach the Middleware to the Publisher Route

```php
use KafkaBus\Core\Bus\Publishers\Router\Options;
use KafkaBus\Commiter\Middleware\ProducerIdempotencyMiddleware;

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

The middleware adds the `x-idempotency-key` header to every outgoing message. If the message does not implement `HasIdempotency`, the header is not added.

## IdempotencyMessageRepository

On the consumer side, `IdempotencyMessageRepository` reads the `x-idempotency-key` header and builds the storage key as `"{header}-{topicName}"`. The same idempotency key in different topics represents different events.

If the header is absent, it falls back to `msgId()`, so legacy producers without idempotency work without any changes.

```php
use KafkaBus\Commiter\Middleware\ConsumerCommiterMiddleware;
use KafkaBus\Commiter\Repositories\IdempotencyMessageRepository;

$repository = new IdempotencyMessageRepository(new DatabaseRepositorySource());

new ConsumerCommiterMiddleware($repository, maxAttempt: 3);
```
