# Configure Producer

## Publishing Flow

```
Application
    │
    ▼ publish(ProducerMessageInterface)
  Bus
    │ → ThreadRegistry → Thread
    │
    ▼
  Publisher
    │ → PublisherRouter (resolves the topic from the message class)
    │
    ▼
  ProducerStream
    │ → ProducerPipelineHandler + route middleware
    │ → ProducerMessage
    ▼
  Producer (low level)
    │ producev() → poll() → flush()
    ▼
  Kafka Broker
```

In more detail:

1. The application calls `Bus::publish($message)` — the message is wrapped in a `MessageBatch`.
2. `Bus` obtains the active `Thread` via `ThreadRegistry`.
3. `Thread` hands the batch to `Publisher`.
4. `PublisherRouter` finds the `Route` by message class and creates (or reuses from cache) a `ProducerStream`.
5. Each message passes through the route's pipeline and turns into a `ProducerMessage`.
6. The low-level `Producer` sends messages via rdkafka and calls `flush()`.

::: tip Stream caching
`PublisherRouter` caches `ProducerStream` by message class. The broker connection and the `RdKafka\ProducerTopic` object are created once per message class, not on every publish.
:::

## Routes

`PublisherRoutesBuilder` maps a message class to a logical topic key:

```php
use KafkaBus\Core\Bus\Publishers\Router\PublisherRoutesBuilder;

$publisherRoutes = PublisherRoutesBuilder::make($topicRegistry)
    ->add(ProductCreatedMessage::class, 'products')
    ->add(OrderCreatedMessage::class, 'orders')
    ->build();
```

Routes are indexed by message class, so:

- one message class maps to exactly one topic;
- calling `add()` again with the same class replaces the previous route;
- publishing a message with no route throws a `RouteProducerException`.

You can inspect the assembled routes via `Bus::routes()` — it returns a `list<Route>` with `messageClass`, `topic`, and `options` fields.

### Routes with Options

Each route can be given its own parameters and middleware:

```php
use KafkaBus\Core\Bus\Publishers\Router\Options;

$publisherRoutes = PublisherRoutesBuilder::make($topicRegistry)
    ->add(
        messageClass: ProductCreatedMessage::class,
        topicKey:     'products',
        options:      new Options(
            additionalOptions: ['compression.codec' => 'gzip'],
            middleware:        [new AuditMiddleware()],
            flushTimeout:      10_000,
            flushRetries:      3,
        )
    )
    ->build();
```

### Route Options

| Parameter            | Type                                | Default | Description                                             |
|------------------------|----------------------------------------|-----------|-------------------------------------------------------------|
| `additionalOptions` | `array<string, scalar\|null>`      | `[]`      | librdkafka parameters for this route's producer            |
| `middleware`        | `list<ProducerPipelineMiddleware>` | `[]`      | Middleware applied to this route's messages                |
| `flushTimeout`      | `int`                                | `5000`    | Timeout for `poll()` and a single `flush()` attempt, ms     |
| `flushRetries`      | `int`                                | `10`      | Number of `flush()` attempts before throwing                |

::: warning Filtering additionalOptions
`additionalOptions` is passed through the `Options::PRODUCER_OPTIONS` allow-list. Keys not on that list (e.g. `group.id` or `auto.offset.reset`) are silently dropped. Full list — in [Configuration](/docs/configuration#filtering-producer-and-consumer-parameters).

Connection-level parameters (`metadata.broker.list`, SASL, etc.) are set in `KafkaConnectionConfig` and aren't subject to this filtering.
:::

## Flush

After `producev()`, the `rdkafka` library delivers messages to the broker asynchronously. `Producer` makes this explicit:

1. for each message — `producev()` with the `RD_KAFKA_MSG_F_BLOCK` flag, then `poll($flushTimeout)`;
2. after the whole batch — `flush($flushTimeout)` inside a `RetryRepeater`.

If `flush()` returns a code other than `RD_KAFKA_RESP_ERR_NO_ERROR`, a `CannotFlushProducerException` is thrown, and `RetryRepeater` retries — up to `flushRetries` times with a 1000 µs pause. Once attempts are exhausted, the exception propagates.

| Parameter      | Default (core) | Default (Laravel) | Description                        |
|-----------------|------------------|----------------------|----------------------------------------|
| `flushTimeout` | `5000` ms        | `5000` ms            | Timeout for a single flush attempt      |
| `flushRetries` | `10`             | `5`                   | Number of attempts before failing       |

::: tip The RD_KAFKA_MSG_F_BLOCK flag
The producer blocks when librdkafka's internal queue is full, instead of dropping messages. This prevents "silent" loss during a load spike, but means `publish()` can stall — take this into account in HTTP handlers.
:::

::: warning
If the process exits before `flush()`, messages may never reach the broker. For critical messages, use the Outbox pattern (the `kafka-bus/outbox` package, in development) — write the event to the database in the same transaction as your business data and dispatch to Kafka separately.
:::

## Example: Different Settings per Topic

```php
use KafkaBus\Core\Bus\Publishers\Router\Options;

$publisherRoutes = PublisherRoutesBuilder::make($topicRegistry)
    // Analytics: maximum compression, some loss is acceptable
    ->add(AnalyticsEvent::class, 'analytics', new Options(
        additionalOptions: ['compression.codec' => 'gzip', 'linger.ms' => 100],
        flushRetries:      1,
    ))
    // Orders: broker-level idempotence, many retries
    ->add(OrderCreatedMessage::class, 'orders', new Options(
        additionalOptions: ['enable.idempotence' => true, 'message.send.max.retries' => 5],
        middleware:        [new ProducerIdempotencyMiddleware()],
        flushRetries:      10,
    ))
    ->build();
```
