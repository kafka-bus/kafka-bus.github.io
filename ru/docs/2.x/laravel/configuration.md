---
title: Конфигурация Laravel
description: Разбор config/kafka-bus.php — подключения, топики, продюсеры, маршруты консьюмера, воркеры и Commiter
---

# Конфигурация

Конфигурация разнесена по двум файлам: `config/kafka-bus.php` (подключения, топики, продюсеры, консьюмеры) и `config/kafka-bus-commiter.php` (хранилище идемпотентности).

## Подключения

```php
'default' => env('KAFKA_CONNECTION', 'kafka'),

'connections' => [
    'kafka' => [
        'driver' => 'kafka',
        'options' => [
            'metadata.broker.list' => env('KAFKA_BROKER_LIST', 'localhost:9092'),
            'security.protocol'    => env('KAFKA_SECURITY_PROTOCOL', 'SASL_PLAINTEXT'),
            'sasl.mechanisms'      => env('KAFKA_SASL_MECHANISMS', 'PLAIN'),
            'sasl.username'        => env('KAFKA_SASL_USERNAME'),
            'sasl.password'        => env('KAFKA_SASL_PASSWORD'),
            'debug'                => env('KAFKA_DEBUG', false),
        ],
    ],

    // Для тестов — не подключается к реальному брокеру
    'testing' => [
        'driver'  => 'null',
        'options' => [],
    ],
],
```

`default` — имя подключения, которое использует шина. Бридж привязывает к нему один `Bus`: через это подключение идут публикация, воркеры и команды оффсетов.

::: info Изменено в 2.0
`KafkaBus::onConnection()` удалён. Чтобы публиковать в другой кластер, соберите отдельный `KafkaBus\Core\Bus` на `app(ConnectionRegistryInterface::class)->connection('analytics')`.
:::

### Драйверы

| Драйвер | Класс конфигурации      | Поведение                                              |
|---------|-------------------------|--------------------------------------------------------|
| `kafka` | `KafkaConnectionConfig` | Реальное подключение через rdkafka                     |
| `null`  | `NullConnectionConfig`  | Публикации отбрасываются, чтение бросает исключение    |

Ключ `debug` обрабатывается особо: он извлекается из `options` и передаётся отдельным флагом (`true` → `debug => all`). Все остальные ключи уходят в librdkafka как есть.

### Собственный драйвер

Вместо имени драйвера можно указать класс-наследник `DriverConnectionFactory` — он будет разрешён через контейнер:

```php
'connections' => [
    'custom' => [
        'driver'  => App\Kafka\Drivers\MyDriverConnectionFactory::class,
        'options' => [/* ... */],
    ],
],
```

Неизвестный драйвер вызывает `KafkaBusConfigurationException`.

## Топики

Физическое имя топика собирается автоматически из `topic_prefix` и значения из `topics`:

```php
'topic_prefix' => env('KAFKA_PREFIX', env('APP_ENV', 'local') . '.'),

'topics' => [
    'products' => 'fact.products.1',
    'orders'   => 'fact.orders.1',
],
```

Итоговое имя топика = `topic_prefix` + значение из `topics`. При `APP_ENV=production`:
- `products` → `production.fact.products.1`
- `orders` → `production.fact.orders.1`

Ключ слева (`products`) — логический, именно он используется в маршрутах, воркерах и командах.

::: warning
Префикс приклеивается конкатенацией без разделителя. Точка в конце `KAFKA_PREFIX` обязательна — иначе получится `productionfact.products.1`.
:::

## Продюсеры

```php
'producers' => [

    // Глобальный middleware для всех маршрутов
    'middleware' => [
        // \KafkaBus\Commiter\Middleware\PublisherIdempotencyMiddleware::class,
    ],

    // Маршруты: класс сообщения → ключ топика
    'routes' => [
        App\Kafka\Messages\ProductMessage::class => 'products',
        App\Kafka\Messages\OrderMessage::class   => 'orders',
    ],

    'flush_timeout' => 5000, // мс
    'flush_retries' => 5,

    'additional_options' => [
        'compression.codec' => env('KAFKA_PRODUCER_COMPRESSION_CODEC', 'snappy'),
    ],
],
```

### Расширенный формат маршрута

Для индивидуальных настроек на уровне маршрута используйте массив вместо строки:

```php
'routes' => [
    App\Kafka\Messages\OrderMessage::class => [
        'topic_key'          => 'orders',
        'middleware'         => [App\Kafka\Middleware\AuditMiddleware::class],
        'additional_options' => ['compression.codec' => 'gzip'],
        'flush_timeout'      => 10_000,
        'flush_retries'      => 3,
    ],
],
```

::: warning
В расширенном формате ключ `topic_key` **обязателен** — без него будет `KafkaBusConfigurationException`. Краткая форма `MessageClass::class => 'orders'` эквивалентна `['topic_key' => 'orders']`.
:::

Публикация класса, которого нет в `routes`, вызывает `RouteProducerException`. Маршруты строятся лениво — при первой публикации каждого класса, — поэтому неверный `topic_key` проявится тогда же или сразу при запуске `php artisan kafka:publisher:route:list`.

## Консьюмеры

Раздел `consumers` состоит из двух независимых частей:

- `routes` — какой обработчик обрабатывает сообщения топика;
- `workers` — какие топики читает процесс `kafka:consume` и с какими параметрами.

```php
'consumers' => [

    // Глобальные middleware, добавляются к каждому маршруту
    'middleware' => [
        // KafkaBus\Commiter\Middleware\ConsumerCommiterMiddleware::class,
    ],

    // Маршруты: ключ топика → обработчик
    'routes' => [
        'products' => App\Kafka\Consumers\ProductsConsumer::class,

        'orders' => [
            'handler'    => App\Kafka\Consumers\OrdersConsumer::class,
            'middleware' => [App\Kafka\Middleware\TenantMiddleware::class],
        ],
    ],

    // Воркеры: имя → топики и параметры консьюмера
    'workers' => [
        'default' => [
            'topics' => ['products', 'orders'],
        ],

        'orders-slow' => [
            'topics'             => ['orders'],
            'consumer_timeout'   => 20_000,
            'additional_options' => ['max.poll.interval.ms' => 900_000],
        ],
    ],

    // Глобальные настройки консьюмера (переопределяются на уровне воркера)
    'auto_commit'      => env('KAFKA_CONSUMER_AUTO_COMMIT', false),
    'consumer_timeout' => 5_000,

    'additional_options' => [
        'group.id'              => env('KAFKA_CONSUMER_GROUP_ID', env('APP_NAME')),
        'max.poll.interval.ms'  => env('KAFKA_MAX_POLL_INTERVAL_MS', 300_000),
        'session.timeout.ms'    => env('KAFKA_SESSION_TIMEOUT_MS', 45_000),
        'heartbeat.interval.ms' => env('KAFKA_HEARTBEAT_INTERVAL_MS', 3_000),
        'auto.offset.reset'     => 'beginning',
    ],
],
```

### Маршруты

Ключ маршрута — ключ топика из `topics`. Значение — класс обработчика или массив:

| Ключ | Обязателен | Описание |
|---|---|---|
| `handler` | да | Класс обработчика, резолвится через контейнер |
| `middleware` | нет | Классы middleware маршрута, добавляются после глобальных `consumers.middleware` |

Обработчики и middleware резолвятся лениво — когда приходит первое сообщение для топика, — поэтому процесс, который только публикует, их не создаёт. У каждого топика ровно один обработчик.

### Воркеры

Ключ воркера — его имя, аргумент команды `php artisan kafka:consume {name}`:

| Ключ | По умолчанию | Описание |
|---|---|---|
| `topics` | `[name]` | Список ключей топиков; если не задан — топик с ключом, равным имени воркера |
| `auto_commit` | глобальное значение | Режим коммита оффсетов, см. [Коммит оффсетов](/ru/docs/2.x/consumer/workers#коммит-оффсетов) |
| `consumer_timeout` | глобальное значение | Сколько `consume()` ждёт сообщение, мс |
| `additional_options` | — | Параметры librdkafka, сливаются с глобальными по ключу |

Объявлять воркер для каждого топика не обязательно: каждый маршрут доступен и как воркер с тем же именем и глобальными параметрами. С конфигом выше `kafka:consume products` читает только топик `products`, хотя записи `products` в `workers` нет.

::: warning
По умолчанию `auto_commit` равен `false` (`KAFKA_CONSUMER_AUTO_COMMIT`): воркер явно коммитит оффсет после каждого успешно обработанного сообщения. При `true` librdkafka коммитит в фоне по таймеру, независимо от результата обработки. Явный коммит требует `kafka-bus/kafka-bus` 2.1.1 или новее — до этой версии при `false` не коммитилось ничего.
:::

### Приоритет настроек

`OptionsMerger` сливает глобальный раздел с настройками воркера или маршрута:

```
глобальные consumers.* / producers.*  →  настройки воркера / маршрута
```

Скалярные ключи (`auto_commit`, `flush_timeout`, …) перекрываются локальным значением. Два ключа обрабатываются особо:

| Ключ | Как сливается |
|---|---|
| `middleware` | Сначала глобальные, затем локальные; класс, указанный в обоих списках, выполняется один раз — на своей локальной позиции |
| `additional_options` | Сливаются по ключу, локальные значения перекрывают глобальные |

::: tip
Глобальный middleware нельзя отключить для отдельного маршрута — списки объединяются, а не заменяются. Если middleware нужен не везде, укажите его в конкретных маршрутах.
:::

### Фильтрация дополнительных параметров

Параметры из `additional_options` проходят через белый список librdkafka: для продюсера — `Options::PRODUCER_OPTIONS`, для консьюмера — `Options::CONSUMER_OPTIONS`. Ключи не из списка молча отбрасываются. Полный перечень — в разделе [Конфигурация](/ru/docs/2.x/configuration#фильтрация-параметров-продюсера-и-консьюмера).

Параметры уровня подключения (`metadata.broker.list`, SASL, `debug`) задаются в секции `connections` и фильтрации не подлежат.

## Commiter

Файл `config/kafka-bus-commiter.php` настраивает хранилище состояний идемпотентности.

```php
return [
    // Активный источник (ключ из "sources")
    'source' => env('KAFKA_COMMITER_SOURCE', 'default'),

    'sources' => [
        'default' => [
            'driver' => 'database',
            'options' => [
                'connection' => env('KAFKA_COMMITER_CONNECTION'), // null = соединение по умолчанию
                'table'      => 'kafka_bus_commits',
            ],
        ],

        'cache' => [
            'driver' => 'redis',
            'options' => [
                'connection' => 'default',
                'prefix'     => 'kafka_bus_commits',
            ],
        ],
    ],

    // Для команды kafka:commit:purge (только database)
    'purge' => [
        'days' => env('KAFKA_COMMITER_PURGE_DAYS', 14),
    ],

    // Стратегия вычисления ключа: "idempotency" | "native"
    'repository' => env('KAFKA_COMMITER_REPOSITORY', 'idempotency'),

    'repositories' => [
        'idempotency' => IdempotencyMessageRepository::class,
        'native'      => NativeMessageRepository::class,
    ],
];
```

### Источники хранения

| Драйвер    | Класс                      | Особенности                                                      |
|------------|----------------------------|------------------------------------------------------------------|
| `database` | `DatabaseRepositorySource` | Таблица `kafka_bus_commits`, `upsert`, поддерживает `purge`      |
| `redis`    | `RedisRepositorySource`    | Хеши с TTL, атомарные Lua-скрипты; очистка через TTL, не `purge` |

::: warning Redis требует PhpRedis
`RedisRepositorySource` работает только с драйвером `phpredis` — при соединении на `predis` провайдер бросит `InvalidArgumentException`. TTL ключей вычисляется как `purge.days × 86400`.
:::

## Все переменные окружения

### kafka-bus.php

| Переменная                         | По умолчанию     | Описание                       |
|------------------------------------|------------------|--------------------------------|
| `KAFKA_CONNECTION`                 | `kafka`          | Активное подключение           |
| `KAFKA_BROKER_LIST`                | `localhost:9092` | Адреса брокеров                |
| `KAFKA_PREFIX`                     | `{APP_ENV}.`     | Префикс имён топиков           |
| `KAFKA_SECURITY_PROTOCOL`          | `SASL_PLAINTEXT` | Протокол безопасности          |
| `KAFKA_SASL_MECHANISMS`            | `PLAIN`          | Механизм SASL                  |
| `KAFKA_SASL_USERNAME`              | —                | Логин SASL                     |
| `KAFKA_SASL_PASSWORD`              | —                | Пароль SASL                    |
| `KAFKA_DEBUG`                      | `false`          | Отладочные логи rdkafka        |
| `KAFKA_CONSUMER_AUTO_COMMIT`       | `false`          | `false` — коммит после обработки, `true` — фоновый коммит |
| `KAFKA_CONSUMER_GROUP_ID`          | `{APP_NAME}`     | Группа консьюмеров             |
| `KAFKA_MAX_POLL_INTERVAL_MS`       | `300000`         | Макс. интервал между опросами  |
| `KAFKA_SESSION_TIMEOUT_MS`         | `45000`          | Таймаут сессии                 |
| `KAFKA_HEARTBEAT_INTERVAL_MS`      | `3000`           | Интервал heartbeat             |
| `KAFKA_PRODUCER_COMPRESSION_CODEC` | `snappy`         | Алгоритм сжатия                |

### kafka-bus-commiter.php

| Переменная                   | По умолчанию  | Описание                                   |
|------------------------------|---------------|--------------------------------------------|
| `KAFKA_COMMITER_SOURCE`      | `default`     | Активный источник хранения                 |
| `KAFKA_COMMITER_CONNECTION`  | —             | Соединение БД (`null` — по умолчанию)      |
| `KAFKA_COMMITER_REPOSITORY`  | `idempotency` | Стратегия вычисления ключа                 |
| `KAFKA_COMMITER_PURGE_DAYS`  | `14`          | Срок хранения записей / TTL ключей Redis   |
