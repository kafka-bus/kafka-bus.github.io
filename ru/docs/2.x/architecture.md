---
title: Архитектура
description: Как устроен Kafka Bus 2.x — шина, роутеры публикации и приёма, воркеры, pipeline и обработка ошибок
---

# Архитектура

Эта страница объясняет, как части Kafka Bus связаны между собой: что делает `Bus`, как сообщение проходит в каждую сторону и почему чтение из Kafka вынесено в отдельный воркер. Читайте её, когда нужно понять поведение за API, а не сам API.

## Ключевые понятия

| Понятие | Класс / интерфейс | Пакет | Описание |
|---|---|---|---|
| **Шина** | `BusInterface`, `Bus` | `core` | Единая точка входа: `publish()` наружу, `dispatch()` внутрь |
| **Подключение** | `ConnectionInterface` | `core` | Настроенное подключение к кластеру Kafka |
| **Топик** | `Topics\Topic` | `core` | Физическое имя топика с логическим ключом |
| **Паблишер** | `Publishers\Publisher` | `core` | Маршрутизирует исходящие сообщения по классу в топик |
| **Ресивер** | `Receivers\ReceiverInterface` | `core` | Маршрутизирует входящие сообщения по топику в обработчик |
| **Pipeline** | `Pipelines\PipelineInterface` | `core` | Цепочка middleware вокруг публикации и обработки |
| **Воркер** | `KafkaBus\Worker\Worker` | `worker` | Именованный список топиков для чтения и параметры консьюмера |
| **Метаданные** | `KafkaBus\Metadata\Metadata` | `metadata` | Топики, партиции и оффсеты групп консьюмеров |

## Симметричная шина

`Bus` привязан к одному подключению и содержит два роутера — по одному на направление:

```
публикация:  Сообщение ──▶ Bus::publish()  ──▶ PublisherRouter ──▶ Kafka
                                              (класс сообщения → топик)

приём:       Kafka ──▶ Worker (poll) ──▶ Bus::dispatch() ──▶ ReceiverRouter ──▶ Обработчик
                                                              (топик → обработчик)
```

Публикация и приём обычно работают в разных процессах ОС — веб-запрос публикует, долгоживущий воркер принимает, — но оба проходят через одну и ту же конфигурацию `Bus`.

Чтение из Kafka намеренно не входит в задачи `Bus`. Воркер знает только, какие топики читать; он получает сырые сообщения и вызывает `$bus->dispatch()`. Об обработчиках, маршрутизации и middleware он ничего не знает. Это значит:

- сообщение можно передать в `Bus` из любого источника — из воркера, теста, ретранслятора Outbox;
- маршруты и middleware объявляются один раз, рядом с маршрутами продюсера;
- цикл чтения можно заменить, не трогая бизнес-код.

## Путь исходящего сообщения

```
$bus->publish($message)
  │  оборачивает в MessageBatch::fromArray([$message])
  ▼
Publisher → PublisherRouter
  │  находит Route по классу сообщения
  ▼
PublisherStream
  │  для каждого сообщения:
  │  PublisherPipelineHandler + middleware маршрута
  │  → ProducerMessage (topic, payload, headers, key, partition)
  ▼
Producer (rdkafka) → producev() → poll() → flush()
```

`PublisherRouter` кэширует `PublisherStream` по классу сообщения, поэтому продюсер для топика создаётся один раз.

## Путь входящего сообщения

```
WorkerRunner::run()
  │  ConsumerStream: subscribe(topics), затем цикл:
  │  consumer->getMessage()
  ▼
$bus->dispatch($message)
  ▼
Receiver → ReceiverRouter
  │  находит Route по физическому имени топика
  ▼
RouteExecutor
  │  MessageFactory: ConsumerMessage → аргумент обработчика
  │  ReceiverPipelineHandler + middleware маршрута
  ▼
Обработчик (ваша бизнес-логика)
  │
  ▼ снова в ConsumerStream: consumer->commit()
```

`ReceiverRouter` кэширует `RouteExecutor` по топику. Коммит выполняет воркер — после того как `dispatch()` успешно вернул управление.

## Маршруты и ленивое построение

Оба роутера наследуют `Routing\AbstractRouter` и берут маршруты из `Routing\RouteCollection`:

- коллекция паблишера индексирована по классу сообщения;
- коллекция ресивера индексирована по физическому имени топика;
- маршрут с уже существующим ключом заменяет старый.

Вместо готового маршрута коллекция может хранить `Routing\LazyRoute`. Ленивый маршрут хранит ключ и замыкание-фабрику; сам маршрут строится при первом `get()` и кэшируется. Laravel-бридж использует это, чтобы получать обработчики и middleware из контейнера только когда приходит первое сообщение для топика.

```php
use KafkaBus\Core\Receivers\Routing\Route;
use KafkaBus\Core\Routing\LazyRoute;
use KafkaBus\Core\Routing\RouteCollection;

$routes = (new RouteCollection(Route::class))
    ->add(new LazyRoute(
        'production.fact.products.1',
        fn (): Route => new Route($topicRegistry->get('products'), new ProductHandler()),
    ));
```

::: warning
Ключ `LazyRoute` должен совпадать с ключом маршрута, который строит фабрика: для маршрутов ресивера это имя топика, для маршрутов паблишера — класс сообщения. При несовпадении `resolve()` бросает `LogicException`.
:::

## Pipeline и middleware

Middleware подключаются в двух местах:

| Направление | Где настраивается | Интерфейс | `target()` | Дополнительно |
|---|---|---|---|---|
| Публикация | `Publishers\Routing\Options::$middleware` | `PublisherPipelineMiddleware` | `ProducerMessageInterface` | `topic()`, `withHeader()` |
| Приём | `Receivers\Routing\RouteInfo::$middleware` | `ReceiverPipelineMiddleware` | `ConsumerMessageInterface` | `formatted()` |

### Как работает pipeline

Каждое сообщение проходит цепочку middleware по порядку. Middleware получает `PipelineInterface` и явно передаёт управление дальше вызовом `continue()`:

```
сообщение → Middleware1 → Middleware2 → Middleware3 → Обработчик
                                                          ↓
            Middleware1 ← Middleware2 ← Middleware3 ← ответ
```

Механика реализована в `Pipeline`:

- `continue()` берёт следующий middleware из очереди и вызывает его; когда очередь пуста, вызывает `handler()->handle()` и сохраняет результат;
- `start()` запускает цепочку и возвращает `result()`;
- повторный `continue()` после завершения бросает `LogicException`.

```php
public function handle(PipelineInterface $pipeline): PipelineInterface
{
    // до обработки
    $pipeline->continue();
    // после обработки

    return $pipeline;
}
```

::: warning Разрыв цепочки
Если middleware вернул `$pipeline`, не вызвав `continue()`, обработчик не запускается, а `result()` остаётся `null`.

При приёме это означает «сообщение пропущено»: воркер всё равно коммитит оффсет. Именно так `ConsumerCommiterMiddleware` пропускает дубли. При публикации сообщение не отправляется: `PublisherStream` пропускает результаты `null`.
:::

## Устойчивость к ошибкам

### Повторы

`Utils\RetryRepeater` повторяет операцию при исключении: до `maximumRetries` попыток с паузой `sleepAfterError` микросекунд (по умолчанию `10` попыток и `1000` мкс). Используется в двух местах:

- `Producer` — повторяет `flush()`, пока доставка не подтверждена;
- `Consumer::commit()` — повторяет коммит оффсета.

### Игнорируемые ошибки консьюмера

`ConsumerStream` воркера не останавливается на штатных «пустых» ответах брокера и продолжает цикл на кодах:

- `RD_KAFKA_RESP_ERR__PARTITION_EOF` — достигнут конец партиции;
- `RD_KAFKA_RESP_ERR__TRANSPORT` — временная сетевая ошибка;
- `RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT`;
- `RD_KAFKA_RESP_ERR__TIMED_OUT` — истёк `consumerTimeout`.

Любой другой код ошибки пробрасывается как `MessageConsumerException`.

### Ошибки обработчика

`Receiver::dispatch()` оборачивает любое исключение маршрутизации, фабрики сообщений, middleware или обработчика в `MessageConsumerNotHandledException`. Исходное исключение доступно через `getPrevious()`, сообщение — через публичное свойство `$consumerMessage`. Воркер его не перехватывает: исключение выходит из `WorkerRunner::run()`, и оффсет этого сообщения не коммитится.

### Иерархия исключений

```
KafkaBus\Core\Exceptions\
├── Connections\
│   ├── ConnectionException                 # подключение не найдено
│   └── DriverException                     # нет драйвера для конфига
├── Consumers\
│   ├── ConsumerException                   # ошибка чтения / null-подключение
│   ├── MessageConsumerException            # сообщение пришло с кодом ошибки
│   ├── MessageConsumerNotHandledException  # dispatch не удался, оборачивает причину
│   └── RouteConsumerException              # нет маршрута для топика
├── Producers\
│   ├── ProducerException
│   ├── CannotFlushProducerException        # flush не подтверждён
│   └── RouteProducerException              # нет маршрута для класса сообщения
└── TopicCannotResolvedException            # ключ топика не зарегистрирован

KafkaBus\Worker\Exceptions\
└── WorkerException                         # воркер не найден в реестре

KafkaBus\Metadata\Exceptions\
├── CannotCommitOffsetException             # оффсет нельзя установить
└── TopicNotFoundException                  # топика нет в кластере
```

## Структура каталогов

```
packages/
├── core/src/
│   ├── Bus.php, BusInterface.php
│   ├── Connections/   # KafkaConnection, NullConnection, ConnectionRegistry, DriverRegistry, Config
│   ├── Consumers/     # Consumer, ConsumerConfig, Messages, Commiters
│   ├── Producers/     # Producer, ProducerConfig, Messages (HasKey, HasHeaders, HasPartition)
│   ├── Publishers/    # Publisher, PublisherStream, MessageBatch, Routing, Pipelines
│   ├── Receivers/     # Receiver, LazyReceiver, Routing, Messages, Attributes, Pipelines
│   ├── Routing/       # AbstractRouter, RouteCollection, LazyRoute
│   ├── Pipelines/     # Pipeline, PipelineBuilder
│   ├── Topics/        # Topic, TopicRegistry
│   ├── Testing/       # BusFaker, ConnectionFaker, MessageFactory, драйверы проверок
│   ├── Exceptions/
│   └── Utils/         # RetryRepeater
├── worker/src/        # Worker, WorkerRunner, WorkerRunnerFactory, WorkerMerger, ConsumerStream
├── metadata/src/      # Metadata, TopicsMetadata, ConsumerGroupMetadata, Partitions
├── messages/src/      # Payload, DomainMessage, JsonMessage, кастеры, фабрики
└── commiter/src/      # ConsumerCommiterMiddleware, PublisherIdempotencyMiddleware, репозитории
```
