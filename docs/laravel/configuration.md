# Конфигурация (Laravel)

Вся конфигурация находится в `config/kafka-bus.php` и разделена на четыре секции.

## Соединения

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

    // Для тестов — не обращается к реальному брокеру
    'testing' => [
        'driver'  => 'null',
        'options' => [],
    ],
],
```

`default` — имя активного соединения. Переключить во время выполнения: `KafkaBus::onConnection('testing')->publish(...)`.

## Топики

В Laravel-пакете физическое имя топика собирается автоматически из `topic_prefix` и значения в `topics`:

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

## Producers

```php
'producers' => [

    // Глобальные middleware для всех маршрутов
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

Для индивидуальных настроек конкретного маршрута используйте массив вместо строки:

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

## Consumers

```php
'consumers' => [

    // Глобальные middleware для всех воркеров
    'middleware' => [
        // \KafkaBus\Commiter\Middleware\ConsumerCommiterMiddleware::class,
    ],

    'workers' => [
        // --- Варианты конфигурации воркера ---

        // 1. Один топик, воркер = ключ топика
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
            'middleware'       => [],
            'auto_commit'      => false,
            'consume_timeout'  => 20_000,
            'topics' => [
                'products' => App\Kafka\Consumers\ProductsConsumer::class,
                'orders'   => [
                    'handler'    => App\Kafka\Consumers\OrdersConsumer::class,
                    'middleware' => [App\Kafka\Middleware\TenantMiddleware::class],
                ],
            ],
        ],
    ],

    // Глобальные настройки consumer'а (переопределяются на уровне воркера)
    'auto_commit'     => env('KAFKA_CONSUMER_AUTO_COMMIT', false),
    'consume_timeout' => 5_000,

    'additional_options' => [
        'group.id'              => env('KAFKA_CONSUMER_GROUP_ID', env('APP_NAME')),
        'max.poll.interval.ms'  => env('KAFKA_MAX_POLL_INTERVAL_MS', 300_000),
        'session.timeout.ms'    => env('KAFKA_SESSION_TIMEOUT_MS', 45_000),
        'heartbeat.interval.ms' => env('KAFKA_HEARTBEAT_INTERVAL_MS', 3_000),
        'auto.offset.reset'     => 'beginning',
    ],
],
```

### Приоритет опций воркера

Опции разрешаются в следующем порядке (от менее к более приоритетному):

```
global consumers.* → worker entry → per-topic (для multi-topic воркера)
```

## Все переменные окружения

| Переменная                         | По умолчанию     | Описание                  |
|------------------------------------|------------------|---------------------------|
| `KAFKA_CONNECTION`                 | `kafka`          | Активное соединение       |
| `KAFKA_BROKER_LIST`                | `localhost:9092` | Адреса брокеров           |
| `KAFKA_PREFIX`                     | `{APP_ENV}.`     | Префикс имён топиков      |
| `KAFKA_SECURITY_PROTOCOL`          | `SASL_PLAINTEXT` | Протокол безопасности     |
| `KAFKA_SASL_MECHANISMS`            | `PLAIN`          | Механизм SASL             |
| `KAFKA_SASL_USERNAME`              | —                | Логин SASL                |
| `KAFKA_SASL_PASSWORD`              | —                | Пароль SASL               |
| `KAFKA_DEBUG`                      | `false`          | Отладочные логи rdkafka   |
| `KAFKA_CONSUMER_AUTO_COMMIT`       | `false`          | Авто-коммит офсетов       |
| `KAFKA_CONSUMER_GROUP_ID`          | `{APP_NAME}`     | Consumer group            |
| `KAFKA_MAX_POLL_INTERVAL_MS`       | `300000`         | Макс. интервал между poll |
| `KAFKA_SESSION_TIMEOUT_MS`         | `45000`          | Таймаут сессии            |
| `KAFKA_HEARTBEAT_INTERVAL_MS`      | `3000`           | Интервал heartbeat        |
| `KAFKA_PRODUCER_COMPRESSION_CODEC` | `snappy`         | Алгоритм сжатия           |
