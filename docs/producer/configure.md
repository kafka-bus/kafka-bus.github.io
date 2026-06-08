# Configure

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
  Producer (низкоуровневый)
    │
    ▼
  Kafka Broker
```

Подробно:

1. Приложение вызывает `Bus::publish($message)`.
2. `Bus` получает активный `Thread` через `ThreadRegistry`.
3. `Thread` передаёт их в `Publisher`.
4. `Publisher` через `PublisherRouter` определяет целевой топик.
5. Сообщение проходит через Pipeline.
6. Низкоуровневый `Producer` отправляет сообщение через rdkafka.

## Маршруты

`PublisherRoutesBuilder` связывает класс сообщения с логическим ключом топика:

```php
use KafkaBus\Core\Bus\Publishers\Router\PublisherRoutesBuilder;

$publisherRoutes = PublisherRoutesBuilder::make($topicRegistry)
    ->add(ProductCreatedMessage::class, 'products')
    ->add(OrderCreatedMessage::class, 'orders')
    ->build();
```

### Маршруты с опциями

Для каждого маршрута можно задать индивидуальные опции и middleware:

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

После отправки сообщений `rdkafka` асинхронно доставляет их в брокер. `flushTimeout` и `flushRetries` контролируют ожидание подтверждения:

| Параметр       | По умолчанию | Описание                    |
|----------------|--------------|-----------------------------|
| `flushTimeout` | `5000` мс    | Таймаут одной попытки flush |
| `flushRetries` | `5`          | Число попыток перед ошибкой |

::: warning
Если процесс завершится до flush, сообщения могут не дойти до брокера. Для критичных сообщений используйте [`kafka-bus/outbox`]).
:::