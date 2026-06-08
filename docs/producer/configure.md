# Configure

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
    │ → PublisherRouter (resolves topic)
    │ → Pipeline
    │
    ▼
  Producer (low-level)
    │
    ▼
  Kafka Broker
```

In detail:

1. The application calls `Bus::publish($message)`.
2. `Bus` retrieves the active `Thread` via `ThreadRegistry`.
3. `Thread` passes them to `Publisher`.
4. `Publisher` resolves the target topic via `PublisherRouter`.
5. The message passes through the Pipeline.
6. The low-level `Producer` sends the message via rdkafka.

## Routes

`PublisherRoutesBuilder` maps a message class to a logical topic key:

```php
use KafkaBus\Core\Bus\Publishers\Router\PublisherRoutesBuilder;

$publisherRoutes = PublisherRoutesBuilder::make($topicRegistry)
    ->add(ProductCreatedMessage::class, 'products')
    ->add(OrderCreatedMessage::class, 'orders')
    ->build();
```

### Routes with Options

Individual options and middleware can be set for each route:

```php
use Micromus\KafkaBus\Bus\Publishers\Router\Options;

$publisherRoutes = PublisherRoutesBuilder::make($topicRegistry)
    ->add(
        messageClass: ProductCreatedMessage::class,
        topicKey:     'products',
        options:      new Options(
            middleware:        [new AuditMiddleware()],
            additionalOptions: ['compression.codec' => 'gzip'],
            flushTimeout:      10_000,
            flushRetries:      3,
        )
    )
    ->build();
```

## Flush

After messages are sent, `rdkafka` delivers them to the broker asynchronously. `flushTimeout` and `flushRetries` control how long to wait for acknowledgement:

| Parameter      | Default   | Description                         |
|----------------|-----------|-------------------------------------|
| `flushTimeout` | `5000` ms | Timeout for a single flush attempt  |
| `flushRetries` | `5`       | Number of attempts before an error  |

::: warning
If the process exits before flushing, messages may not reach the broker. For critical messages, use [`kafka-bus/outbox`]).
:::
