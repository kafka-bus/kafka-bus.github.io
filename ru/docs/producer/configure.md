# Настройка продюсера

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
    │ → PublisherRouter (определяет топик по классу сообщения)
    │
    ▼
  ProducerStream
    │ → ProducerPipelineHandler + middleware маршрута
    │ → ProducerMessage
    ▼
  Producer (низкий уровень)
    │ producev() → poll() → flush()
    ▼
  Kafka Broker
```

Подробнее:

1. Приложение вызывает `Bus::publish($message)` — сообщение оборачивается в `MessageBatch`.
2. `Bus` получает активный `Thread` через `ThreadRegistry`.
3. `Thread` передаёт батч в `Publisher`.
4. `PublisherRouter` находит `Route` по классу сообщения и создаёт (либо берёт из кеша) `ProducerStream`.
5. Каждое сообщение проходит через pipeline маршрута и превращается в `ProducerMessage`.
6. Низкоуровневый `Producer` отправляет сообщения через rdkafka и выполняет `flush()`.

::: tip Кеширование потоков
`PublisherRouter` кеширует `ProducerStream` по классу сообщения. Соединение с брокером и объект `RdKafka\ProducerTopic` создаются один раз на класс сообщения, а не на каждую публикацию.
:::

## Маршруты

`PublisherRoutesBuilder` маппит класс сообщения на логический ключ топика:

```php
use KafkaBus\Core\Bus\Publishers\Router\PublisherRoutesBuilder;

$publisherRoutes = PublisherRoutesBuilder::make($topicRegistry)
    ->add(ProductCreatedMessage::class, 'products')
    ->add(OrderCreatedMessage::class, 'orders')
    ->build();
```

Маршруты индексируются по классу сообщения, поэтому:

- один класс сообщения → ровно один топик;
- повторный `add()` с тем же классом заменит предыдущий маршрут;
- публикация сообщения без маршрута бросает `RouteProducerException`.

Просмотреть собранные маршруты можно через `Bus::routes()` — метод возвращает `list<Route>` с полями `messageClass`, `topic` и `options`.

### Маршруты с параметрами

Для каждого маршрута можно задать индивидуальные параметры и middleware:

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

### Параметры маршрута

| Параметр            | Тип                            | По умолчанию | Описание                                              |
|---------------------|--------------------------------|--------------|-------------------------------------------------------|
| `additionalOptions` | `array<string, scalar\|null>`  | `[]`         | Параметры librdkafka для продюсера этого маршрута    |
| `middleware`        | `list<ProducerPipelineMiddleware>` | `[]`     | Middleware, применяемые к сообщениям этого маршрута   |
| `flushTimeout`      | `int`                          | `5000`       | Таймаут `poll()` и одной попытки `flush()`, мс        |
| `flushRetries`      | `int`                          | `10`         | Количество попыток `flush()` перед исключением        |

::: warning Фильтрация additionalOptions
`additionalOptions` пропускаются через белый список `Options::PRODUCER_OPTIONS`. Ключи, которых там нет (например, `group.id` или `auto.offset.reset`), молча отбрасываются. Полный список — в разделе [Конфигурация](/ru/docs/configuration#фильтрация-параметров-продюсера-и-консьюмера).

Параметры уровня подключения (`metadata.broker.list`, SASL и т. п.) задаются в `KafkaConnectionConfig` и фильтрации не подлежат.
:::

## Сброс буфера (flush)

После `producev()` библиотека `rdkafka` доставляет сообщения брокеру асинхронно. `Producer` делает это явно:

1. для каждого сообщения — `producev()` с флагом `RD_KAFKA_MSG_F_BLOCK`, затем `poll($flushTimeout)`;
2. после всего батча — `flush($flushTimeout)` внутри `RetryRepeater`.

Если `flush()` вернул код, отличный от `RD_KAFKA_RESP_ERR_NO_ERROR`, бросается `CannotFlushProducerException`, и `RetryRepeater` повторяет попытку — до `flushRetries` раз с паузой 1000 мкс. Исчерпав попытки, исключение пробрасывается наружу.

| Параметр       | По умолчанию (ядро) | По умолчанию (Laravel) | Описание                         |
|----------------|---------------------|------------------------|----------------------------------|
| `flushTimeout` | `5000` мс           | `5000` мс              | Таймаут одной попытки flush      |
| `flushRetries` | `10`                | `5`                    | Количество попыток перед ошибкой |

::: tip Флаг RD_KAFKA_MSG_F_BLOCK
Продюсер блокируется, когда внутренняя очередь librdkafka переполнена, вместо того чтобы отбрасывать сообщения. Это исключает «тихую» потерю при всплеске нагрузки, но означает, что `publish()` может подвиснуть — учитывайте это в HTTP-обработчиках.
:::

::: warning
Если процесс завершится до `flush()`, сообщения могут не достичь брокера. Для критичных сообщений используйте паттерн Outbox (пакет `kafka-bus/outbox` в разработке) — запись события в БД в одной транзакции с бизнес-данными и отдельная отправка в Kafka.
:::

## Пример: разные настройки для разных топиков

```php
use KafkaBus\Core\Bus\Publishers\Router\Options;

$publisherRoutes = PublisherRoutesBuilder::make($topicRegistry)
    // Аналитика: максимальное сжатие, потеря допустима
    ->add(AnalyticsEvent::class, 'analytics', new Options(
        additionalOptions: ['compression.codec' => 'gzip', 'linger.ms' => 100],
        flushRetries:      1,
    ))
    // Заказы: идемпотентность на уровне брокера, много попыток
    ->add(OrderCreatedMessage::class, 'orders', new Options(
        additionalOptions: ['enable.idempotence' => true, 'message.send.max.retries' => 5],
        middleware:        [new ProducerIdempotencyMiddleware()],
        flushRetries:      10,
    ))
    ->build();
```
