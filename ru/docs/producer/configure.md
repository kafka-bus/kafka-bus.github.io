# Настройка Producer

## Поток публикации

```
Приложение
    │
    ▼ publish(ProducerMessageInterface)
  Bus
    │ → ThreadRegistry → Thread
    │
    ▼
  Publisher
    │ → PublisherRouter (определяет топик)
    │ → Pipeline
    │
    ▼
  Producer (низкий уровень)
    │
    ▼
  Kafka Broker
```

Подробнее:

1. Приложение вызывает `Bus::publish($message)`.
2. `Bus` получает активный `Thread` через `ThreadRegistry`.
3. `Thread` передаёт сообщение в `Publisher`.
4. `Publisher` определяет целевой топик через `PublisherRouter`.
5. Сообщение проходит через Pipeline.
6. Низкоуровневый `Producer` отправляет сообщение через rdkafka.

## Маршруты

`PublisherRoutesBuilder` маппит класс сообщения на логический ключ топика:

```php
use KafkaBus\Core\Bus\Publishers\Router\PublisherRoutesBuilder;

$publisherRoutes = PublisherRoutesBuilder::make($topicRegistry)
    ->add(ProductCreatedMessage::class, 'products')
    ->add(OrderCreatedMessage::class, 'orders')
    ->build();
```

### Маршруты с параметрами

Для каждого маршрута можно задать индивидуальные параметры и middleware:

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

После отправки сообщений `rdkafka` доставляет их брокеру асинхронно. `flushTimeout` и `flushRetries` контролируют время ожидания подтверждения:

| Параметр       | По умолчанию | Описание                                  |
|----------------|--------------|-------------------------------------------|
| `flushTimeout` | `5000` мс    | Таймаут одной попытки flush               |
| `flushRetries` | `5`          | Количество попыток перед ошибкой          |

::: warning
Если процесс завершится до flush, сообщения могут не достичь брокера. Для критических сообщений используйте [`kafka-bus/outbox`]).
:::
