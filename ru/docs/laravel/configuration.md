# Конфигурация

Конфигурация разнесена по двум файлам: `config/kafka-bus.php` (подключения, топики, продьюсеры, консьюмеры) и `config/kafka-bus-commiter.php` (хранилище идемпотентности).

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

`default` — имя активного подключения. Переключение во время выполнения: `KafkaBus::onConnection('testing')->publish(...)`.

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

## Продьюсеры

```php
'producers' => [

    // Глобальный middleware для всех маршрутов
    'middleware' => [
        // \KafkaBus\Commiter\Middleware\ProducerIdempotencyMiddleware::class,
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

Публикация класса, которого нет в `routes`, тоже вызывает `KafkaBusConfigurationException` — при создании маршрута, а не при отправке.

## Консьюмеры

```php
'consumers' => [

    // Глобальный middleware для всех воркеров
    'middleware' => [
        // \KafkaBus\Commiter\Middleware\ConsumerCommiterMiddleware::class,
    ],

    'workers' => [
        // --- Варианты конфигурации воркера ---

        // 1. Один топик, имя воркера = ключ топика
        'products' => App\Kafka\Consumers\ProductsConsumer::class,

        // 2. Один топик с переопределениями
        'orders' => [
            'middleware' => [],
            'handler'    => App\Kafka\Consumers\OrdersConsumer::class,
        ],

        // 3. Один топик, имя воркера ≠ ключ топика
        'products-secondary' => [
            'topic_key'  => 'products',
            'middleware' => [],
            'handler'    => App\Kafka\Consumers\ProductsSecondaryConsumer::class,
        ],

        // 4. Несколько топиков в одном воркере
        'default' => [
            'middleware'      => [],
            'auto_commit'     => false,
            'consumer_timeout'=> 20_000,
            'topics' => [
                'products' => App\Kafka\Consumers\ProductsConsumer::class,
                'orders'   => [
                    'handler'    => App\Kafka\Consumers\OrdersConsumer::class,
                    'middleware' => [App\Kafka\Middleware\TenantMiddleware::class],
                ],
            ],
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

Как распознаётся формат воркера:

| Значение                        | Интерпретация                                        |
|---------------------------------|------------------------------------------------------|
| строка (класс)                  | Один топик, ключ топика = имя воркера                |
| массив с ключом `handler`       | Один топик; ключ берётся из `topic_key` или из имени |
| массив с ключом `topics`        | Несколько топиков                                    |

### Приоритет настроек

`OptionsMerger` объединяет глобальную секцию с настройками воркера или маршрута:

```
global consumers.* / producers.*  →  настройки воркера / маршрута
```

Скалярные ключи (`auto_commit`, `flush_timeout`, …) перекрываются локальным значением. Два ключа обрабатываются особо:

| Ключ                 | Поведение при слиянии                                          |
|----------------------|-----------------------------------------------------------------|
| `middleware`         | Объединяются: сначала глобальные (без дублей), затем локальные  |
| `additional_options` | Объединяются по ключам, локальные перекрывают глобальные        |

::: tip
Глобальный middleware нельзя отключить для отдельного воркера — списки складываются, а не замещаются. Если middleware нужен не везде, не объявляйте его глобально, а перечисляйте в конкретных воркерах.
:::

Middleware выполняются в порядке итогового списка: глобальные первыми. Для `ConsumerCommiterMiddleware` это удачно — проверка дублей происходит раньше остальной обвязки.

### Фильтрация additional_options

Параметры из `additional_options` проходят через белый список librdkafka: для продьюсера — `Options::PRODUCER_OPTIONS`, для консьюмера — `Options::CONSUMER_OPTIONS`. Ключи не из списка молча отбрасываются. Полный перечень — в разделе [Конфигурация ядра](/ru/docs/configuration#фильтрация-параметров-producer-vs-consumer).

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
| `KAFKA_CONSUMER_AUTO_COMMIT`       | `false`          | Явный коммит после обработки   |
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
