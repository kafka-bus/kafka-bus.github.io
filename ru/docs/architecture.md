# Архитектура

## Ключевые концепции

| Концепция      | Класс / интерфейс              | Описание                                                             |
|----------------|--------------------------------|----------------------------------------------------------------------|
| **Bus**        | `KafkaBus\Core\Bus`            | Центральный фасад. Единая точка входа для публикации и прослушивания |
| **Connection** | `ConnectionInterface`          | Настроенное подключение к кластеру Kafka                             |
| **Thread**     | `ThreadInterface`              | Логическая обёртка над подключением, содержит Publisher и Listener   |
| **Publisher**  | `Bus\Publishers\Publisher`     | Высокоуровневая абстракция для отправки сообщений                    |
| **Producer**   | `ProducerInterface`            | Низкоуровневый компонент, взаимодействующий с rdkafka                |
| **Listener**   | `ListenerInterface`            | Высокоуровневая абстракция для прослушивания топиков                 |
| **Consumer**   | `ConsumerInterface`            | Низкоуровневый компонент чтения через rdkafka                        |
| **Worker**     | `Bus\Listeners\Workers\Worker` | Именованный набор маршрутов консьюмера и его настроек                |
| **Topic**      | `Topics\Topic`                 | Именованный канал в Kafka с логическим ключом                        |
| **Pipeline**   | `PipelineInterface`            | Middleware pipeline для входящих и исходящих сообщений               |

## Структура директорий

```
src/
├── Bus/              # Фасад Bus, Thread, ThreadRegistry, ThreadFactory, MessageBatch
│   ├── Listeners/    # ListenerFactory, Workers, Partitions (offset-операции)
│   └── Publishers/   # PublisherFactory, Router (PublisherRoutesBuilder, Options)
├── Connections/      # KafkaConnection, NullConnection, ConnectionRegistry, DriverRegistry
├── Consumers/        # ConsumerStream, Router, Handlers, Messages, Commiters, Attributes
├── Producers/        # Producer, ProducerStream, Messages, Pipelines
├── Interfaces/       # Контракты для всех ключевых компонентов
├── Pipelines/        # Pipeline и PipelineBuilder
├── Topics/           # Topic, TopicRegistry
├── Testing/          # ConnectionFaker, ProducerFaker, ConsumerFaker, MessageFactory
├── Exceptions/       # Иерархия исключений
└── Support/          # RetryRepeater
```

## Подключения и потоки

`Bus` поддерживает множество именованных подключений. Каждое подключение живёт в своём `Thread`:

```
Bus
├── Thread[default]    ← ConnectionRegistry['kafka-main']
│   ├── Publisher
│   └── Listener
└── Thread[analytics]  ← ConnectionRegistry['kafka-analytics']
    ├── Publisher
    └── Listener
```

`Thread` — не системный поток ОС, а логическая обёртка над подключением. `ThreadRegistry` создаёт их лениво и кеширует по имени подключения, так что повторный `onConnection('analytics')` вернёт тот же объект.

Важное следствие: `Publisher` создаётся сразу в конструкторе `Thread`, а `Listener` — только при вызове `listener()`. Таблица маршрутов продюсера `PublisherRoutes` при этом общая для всех подключений: она передаётся в `PublisherFactory` один раз.

## Путь исходящего сообщения

```
$bus->publish($message)
  │
  ▼ Bus → Thread → Publisher → PublisherRouter
  │        находит Route по классу сообщения
  ▼
ProducerStream
  │  для каждого сообщения:
  │  ProducerPipelineHandler + middleware маршрута
  │  → ProducerMessage (payload, headers, key, partition)
  ▼
Producer (rdkafka) → produce() → poll() → flush()
```

`PublisherRouter` кеширует `ProducerStream` по классу сообщения, поэтому продюсер и его соединение создаются один раз на класс.

## Путь входящего сообщения

```
$bus->listener('worker')->listen()
  │
  ▼ ListenerFactory → Worker из WorkerRegistry
  ▼ ConsumerStreamFactory → ConsumerStream + MessageHandler
  │
  ▼ бесконечный цикл:
ConsumerStream::listen()
  │  consumer->getMessage()
  ▼
MessageHandler  ─── Consumer Pipeline (middleware воркера)
  │                  target: ConsumerMessageInterface
  ▼
ConsumerRouter  ─── находит Route по имени топика
  │
  ▼ MessageFactory: ConsumerMessage → тип аргумента обработчика
  │
  ▼ Route Pipeline (middleware маршрута)
  ▼
Обработчик (ваша бизнес-логика)
  │
  ▼ consumer->commit()
```

Два pipeline — не дублирование: первый видит сырое сообщение Kafka и общий для всех топиков воркера, второй видит уже десериализованный объект и настраивается для каждого топика отдельно.

## Pipeline и middleware

Middleware можно добавить на трёх уровнях:

| Уровень          | Где задаётся                          | Интерфейс                     | Что видит `target()`         |
|------------------|---------------------------------------|-------------------------------|------------------------------|
| Воркер           | `Workers\Options::$middleware`        | `ConsumerPipelineMiddleware`  | `ConsumerMessageInterface`   |
| Маршрут консьюмера | `RouteInfo::$middleware`            | `MessagePipelineMiddleware`   | результат `MessageFactory`   |
| Маршрут продюсера | `Publishers\Router\Options::$middleware` | `ProducerPipelineMiddleware` | `ProducerMessageInterface` |

### Как работает Pipeline

Каждое сообщение проходит через цепочку middleware по порядку. Каждый middleware получает `PipelineInterface` и явно передаёт управление дальше вызовом `continue()`:

```
message → Middleware1 → Middleware2 → Middleware3 → Handler
                                                        ↓
          Middleware1 ← Middleware2 ← Middleware3 ← response
```

Механика реализована в `Pipeline`:

- `continue()` снимает первый middleware из очереди и вызывает его; когда очередь пуста — выполняет `handler()->handle()` и запоминает результат;
- `start()` запускает цепочку и возвращает `result()`;
- повторный `continue()` после завершения бросает `LogicException`.

```php
public function handle(PipelineInterface $pipeline): PipelineInterface
{
    // до обработки
    $pipeline->continue();   // передать управление дальше
    // после обработки

    return $pipeline;
}
```

::: warning Прерывание цепочки
Если middleware вернёт `$pipeline` **без** вызова `continue()`, обработчик не выполнится, а `result()` останется `null`.

Для консьюмера это значит «сообщение пропущено» (именно так работает `ConsumerCommiterMiddleware` для дублей). Для продюсера — сообщение **не будет отправлено**: `ProducerStream` пропускает `null`-результаты.
:::

## Устойчивость к ошибкам

### RetryRepeater

`Support\RetryRepeater` рекурсивно повторяет операцию при исключении: до `maximumRetries` попыток с паузой `sleepAfterError` микросекунд (по умолчанию `10` попыток и `1000` мкс). Используется в двух местах:

- `Producer::flush()` — повтор подтверждения отправки;
- `Consumer::commit()` — повтор коммита offset.

### Игнорируемые ошибки консьюмера

`ConsumerStream` не роняет воркер на штатных «пустых» ответах брокера и продолжает цикл при следующих кодах:

- `RD_KAFKA_RESP_ERR__PARTITION_EOF` — достигнут конец партиции;
- `RD_KAFKA_RESP_ERR__TRANSPORT` — временная сетевая ошибка;
- `RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT`;
- `RD_KAFKA_RESP_ERR__TIMED_OUT` — истёк `consumerTimeout`.

Любой другой код ошибки пробрасывается наружу как `MessageConsumerException`.

### Иерархия исключений

```
Exceptions/
├── Connections/
│   ├── ConnectionException            # подключение не найдено
│   └── DriverException                # драйвер для конфига не зарегистрирован
├── Consumers/
│   ├── ConsumerException              # ошибка чтения / null-подключение
│   ├── MessageConsumerException       # сообщение пришло с кодом ошибки
│   ├── MessageConsumerNotHandledException  # обработчик выбросил исключение
│   └── RouteConsumerException         # нет маршрута для топика
├── Producers/
│   ├── ProducerException
│   ├── CannotFlushProducerException   # flush не подтверждён
│   └── RouteProducerException         # нет маршрута для класса сообщения
├── Listeners/
│   ├── ListenerException              # воркер не найден
│   └── CannotCommitOffsetException    # offset вне допустимых границ
├── TopicCannotResolvedException       # ключ топика не зарегистрирован
└── TopicNotFoundException             # топик отсутствует в кластере
```

Любое исключение из обработчика оборачивается в `MessageConsumerNotHandledException` с оригинальным сообщением в `previous`.

## Несколько воркеров в одном процессе

`Bus::listener()` принимает как имя воркера, так и массив имён:

```php
$bus->listener('products')->listen();
$bus->listener(['products', 'orders'])->listen();
```

При передаче массива `WorkerMerger` объединяет воркеров в один синтетический воркер `grouped-worker`: маршруты всех воркеров складываются в общую таблицу, а обработчиком каждого маршрута становится `RouteHandler`, который запускает *собственный* `MessageHandler` исходного воркера. Так сохраняется индивидуальный набор middleware каждого воркера.

::: warning
Настройки соединения (`additionalOptions`, `autoCommit`, `consumerTimeout`) при объединении берутся **только у первого воркера** в списке. Настройки остальных игнорируются, поэтому объединять имеет смысл воркеров с одинаковым `group.id` и режимом коммита.
:::
