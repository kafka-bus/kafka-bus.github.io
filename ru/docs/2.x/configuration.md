---
title: Конфигурация
description: Подключения к Kafka, SASL, null-драйвер, фильтрация параметров и сборка шины в Kafka Bus 2.x
---

# Конфигурация

Пакет строится вокруг `ConnectionRegistry` — реестра именованных подключений к Kafka. Каждое подключение описывается объектом конфигурации `ConnectionConfigInterface`, а сам объект подключения `ConnectionInterface` создаётся лениво драйвером из `DriverRegistry` при первом обращении.

```
ConnectionRegistry
  ├── 'kafka'   → KafkaConnectionConfig ──[DriverRegistry]──▶ KafkaConnection
  └── 'testing' → NullConnectionConfig  ──[DriverRegistry]──▶ NullConnection
```

## Подключения

### Подключение к Kafka

`KafkaConnectionConfig` — конфигурация подключения к реальному брокеру:

```php
use KafkaBus\Core\Connections\Config\KafkaConnectionConfig;
use KafkaBus\Core\Connections\Registry\ConnectionRegistry;

$config = new KafkaConnectionConfig(
    brokerList: 'localhost:9092',
);

$connectionRegistry = new ConnectionRegistry([
    'kafka' => $config,
]);
```

Параметры конструктора:

| Параметр     | Тип                           | По умолчанию | Описание                                                       |
|--------------|-------------------------------|--------------|----------------------------------------------------------------|
| `brokerList` | `string`                      | —            | Значение `metadata.broker.list`, список брокеров через запятую |
| `logLevel`   | `int`                         | `LOG_DEBUG`  | Значение `log_level` для librdkafka                            |
| `debug`      | `bool`                        | `false`      | При `true` включает `debug => all`                             |
| `saslConfig` | `?UserCredentialsConfig`      | `null`       | Учётные данные SASL                                            |
| `extra`      | `array<string, scalar\|null>` | `[]`         | Любые дополнительные параметры librdkafka                      |

### Подключение с SASL-аутентификацией

Учётные данные задаются отдельным объектом `UserCredentialsConfig`, а не «сырыми» ключами:

```php
use KafkaBus\Core\Connections\Config\KafkaConnectionConfig;
use KafkaBus\Core\Connections\Config\UserCredentialsConfig;

$config = new KafkaConnectionConfig(
    brokerList: 'kafka.example.com:9092',
    saslConfig: new UserCredentialsConfig(
        username: 'my-user',
        password: 'my-password',
        protocol: 'SASL_SSL', // по умолчанию 'plaintext'
        mechanisms: 'SCRAM-SHA-512', // по умолчанию PLAIN
    ),
);
```

`UserCredentialsConfig::toOptions()` разворачивается в четыре параметра librdkafka:

```php
[
    'security.protocol' => $protocol,
    'sasl.mechanisms'   => $mechanisms,
    'sasl.username'     => $username,
    'sasl.password'     => $password,
]
```

### Порядок слияния параметров

`KafkaConnectionConfig::getOptions()` собирает итоговый набор в таком порядке (каждый следующий перекрывает предыдущий):

```
базовые (metadata.broker.list, log_level, debug) → saslConfig → extra
```

### Несколько подключений

`ConnectionRegistry` поддерживает несколько именованных подключений — например, к разным кластерам:

```php
$connectionRegistry = new ConnectionRegistry([
    'main'      => new KafkaConnectionConfig('kafka-main:9092'),
    'analytics' => new KafkaConnectionConfig('kafka-analytics:9092'),
]);
```

Подключения создаются лениво и кэшируются: повторный запрос по тому же имени возвращает тот же объект. Запрос незарегистрированного имени бросает `ConnectionException` со списком доступных подключений.

`Bus` работает ровно с одним подключением. Чтобы публиковать в два кластера, соберите шину для каждого из них:

```php
$mainBus      = new Bus($connectionRegistry->connection('main'), $publisherFactory);
$analyticsBus = new Bus($connectionRegistry->connection('analytics'), $publisherFactory);
```

::: info Изменено в 2.0
`Bus::onConnection()` удалён — см. [Переход с 1.x](/ru/docs/2.x/upgrade#несколько-подключении).
:::

### Null-драйвер

`NullConnection` принимает публикации и молча их отбрасывает `NullProducer`. Создание консьюмера на таком подключении бросает `ConsumerException` — читать через него нельзя.

```php
use KafkaBus\Core\Connections\Config\NullConnectionConfig;

$connectionRegistry = new ConnectionRegistry([
    'testing' => new NullConnectionConfig(),
]);
```

::: tip
В пакете для Laravel драйвер `null` настраивается через `'driver' => 'null'` в конфиге — создавать отдельный класс не нужно.

Для тестов, где нужно ещё и *читать* сообщения, используйте `BusFaker` — см. [Тестирование консьюмера](/ru/docs/2.x/consumer/testing).
:::

### Быстрый старт без ручной сборки

Для локальной разработки есть готовый реестр с подключениями `default` и `null`:

```php
use KafkaBus\Core\Connections\Registry\ConnectionRegistry;

$connectionRegistry = ConnectionRegistry::default(); // 127.0.0.1:29092
$connectionRegistry = ConnectionRegistry::default('localhost', 9092);
```

### Собственный драйвер

`DriverRegistry` сопоставляет класс конфигурации с фабрикой подключения. Чтобы добавить свой транспорт, зарегистрируйте пару «класс конфига → фабрика»:

```php
use KafkaBus\Core\Connections\Registry\DriverRegistry;

$driverRegistry = new DriverRegistry(); // уже содержит kafka и null

$driverRegistry->add(
    MyConnectionConfig::class,
    static fn (string $name, MyConnectionConfig $config) => new MyConnection($name, $config),
);

$connectionRegistry = new ConnectionRegistry(
    ['custom' => new MyConnectionConfig()],
    $driverRegistry,
);
```

Если для класса конфигурации не найден драйвер, выбрасывается `DriverException` со списком зарегистрированных классов.

## Фильтрация параметров продюсера и консьюмера

Деталь, о которой легко забыть: параметры подключения `KafkaConnectionConfig` применяются и к продюсеру, и к консьюмеру целиком, а дополнительные параметры маршрута или воркера фильтруются по белому списку.

`Options::getProducerOptions()` и `Options::getConsumerOptions()` пропускают только те ключи из `additionalOptions`, которые есть в соответствующем списке:

```php
use KafkaBus\Core\Publishers\Routing\Options as RouteOptions;
use KafkaBus\Worker\Options as WorkerOptions;

// Попадёт в продюсер
new RouteOptions(additionalOptions: ['compression.codec' => 'gzip']);

// Будет молча отброшено: 'compression.codec' отсутствует в CONSUMER_OPTIONS
new WorkerOptions(additionalOptions: ['compression.codec' => 'gzip']);
```

::: warning
Неизвестный или «чужой» ключ не вызывает ошибку — он просто отбрасывается. Если параметр не применяется, первым делом проверьте, что он есть в нужном списке.
:::

::: details Разрешённые ключи продюсера (Options::PRODUCER_OPTIONS)
`transactional.id`, `transaction.timeout.ms`, `enable.idempotence`, `enable.gapless.guarantee`, `queue.buffering.max.messages`, `queue.buffering.max.kbytes`, `queue.buffering.max.ms`, `linger.ms`, `message.send.max.retries`, `retries`, `retry.backoff.ms`, `queue.buffering.backpressure.threshold`, `compression.codec`, `compression.type`, `batch.num.messages`, `batch.size`, `delivery.report.only.error`, `dr_cb`, `dr_msg_cb`, `sticky.partitioning.linger.ms`
:::

::: details Разрешённые ключи консьюмера (Options::CONSUMER_OPTIONS)
`partition.assignment.strategy`, `session.timeout.ms`, `heartbeat.interval.ms`, `group.protocol.type`, `coordinator.query.interval.ms`, `max.poll.interval.ms`, `enable.auto.commit`, `auto.commit.interval.ms`, `enable.auto.offset.store`, `queued.min.messages`, `queued.max.messages.kbytes`, `fetch.wait.max.ms`, `fetch.message.max.bytes`, `max.partition.fetch.bytes`, `fetch.max.bytes`, `fetch.min.bytes`, `fetch.error.backoff.ms`, `offset.store.method`, `isolation.level`, `consume_cb`, `rebalance_cb`, `offset_commit_cb`, `enable.partition.eof`, `check.crcs`, `allow.auto.create.topics`, `auto.offset.reset`, `group.id`
:::

### Нормализация значений

Перед передачей в librdkafka булевы значения приводятся к строкам, после чего набор пропускается через `array_filter()` — falsy-значения отбрасываются:

| Значение в PHP | Что уйдёт в librdkafka |
|----------------|------------------------|
| `true`         | `'true'`               |
| `false`        | `'false'`              |
| `null`         | отбрасывается          |
| `0`            | отбрасывается          |
| `'0'`          | отбрасывается          |
| `''`           | отбрасывается          |

Булевы значения безопасны: `false` сначала становится непустой строкой `'false'` и переживает фильтрацию.

::: warning
А вот числовой `0` теряется. Параметры вида `'linger.ms' => 0` или `'queue.buffering.max.ms' => 0` до брокера не дойдут — передавайте их строкой: `'linger.ms' => '0'`.
:::

## Сборка шины

Когда реестр подключений настроен, соберите `Bus` — единую точку входа для публикации и приёма сообщений:

```php
use KafkaBus\Core\Bus;
use KafkaBus\Core\Publishers\PublisherFactory;

$bus = new Bus(
    connection: $connectionRegistry->connection('kafka'),
    publisherFactory: new PublisherFactory(routes: $publisherRoutes),
    receiver: $receiver,
);
```

Параметры конструктора:

| Параметр | Тип | По умолчанию | Описание |
|---|---|---|---|
| `connection` | `ConnectionInterface` | — | Подключение для публикации; через него же воркеры читают сообщения |
| `publisherFactory` | `PublisherFactory` | `new PublisherFactory()` | Таблица маршрутов продюсера, см. [Настройка продюсера](/ru/docs/2.x/producer/configure) |
| `receiver` | `ReceiverInterface` | `new Receiver()` | Таблица маршрутов консьюмера, см. [Маршруты консьюмера](/ru/docs/2.x/consumer/configure) |

Обе таблицы необязательны: сервису, который только публикует, ресивер не нужен, а сервису, который только читает, не нужны маршруты продюсера.

Сам `Bus` никогда не читает Kafka. Чтением занимается воркер из `kafka-bus/worker`: он берёт сообщения из `$bus->connection()` и передаёт их в `$bus->dispatch()` — см. [Воркеры](/ru/docs/2.x/consumer/workers).

## Справочник параметров

Наиболее часто используемые параметры librdkafka:

| Параметр               | Уровень            | Описание                  | Пример                   |
|------------------------|--------------------|---------------------------|--------------------------|
| `metadata.broker.list` | подключение        | Адреса брокеров           | `kafka:9092,kafka2:9092` |
| `security.protocol`    | подключение        | Протокол безопасности     | `SASL_PLAINTEXT`, `SSL`  |
| `sasl.mechanisms`      | подключение        | Механизм SASL             | `PLAIN`, `SCRAM-SHA-256` |
| `debug`                | подключение        | Отладочные логи rdkafka   | `consumer,cgrp,topic`    |
| `compression.codec`    | продюсер          | Сжатие                    | `snappy`, `gzip`, `lz4`  |
| `linger.ms`            | продюсер          | Задержка перед батчингом  | `10`                     |
| `group.id`             | консьюмер          | Группа потребителей       | `my-service`             |
| `auto.offset.reset`    | консьюмер          | Начальная позиция         | `earliest`, `latest`     |
| `max.poll.interval.ms` | консьюмер          | Таймаут опроса            | `300000`                 |
| `session.timeout.ms`   | консьюмер          | Таймаут сессии            | `45000`                  |

Полный список — в [документации librdkafka](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).
