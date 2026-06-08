# Configuration (Laravel)

All configuration lives in `config/kafka-bus.php` and is divided into four sections.

## Connections

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

    // For tests — does not connect to a real broker
    'testing' => [
        'driver'  => 'null',
        'options' => [],
    ],
],
```

`default` — the name of the active connection. Switch at runtime: `KafkaBus::onConnection('testing')->publish(...)`.

## Topics

In the Laravel package, the physical topic name is assembled automatically from `topic_prefix` and the value in `topics`:

```php
'topic_prefix' => env('KAFKA_PREFIX', env('APP_ENV', 'local') . '.'),

'topics' => [
    'products' => 'fact.products.1',
    'orders'   => 'fact.orders.1',
],
```

Final topic name = `topic_prefix` + value from `topics`. With `APP_ENV=production`:
- `products` → `production.fact.products.1`
- `orders` → `production.fact.orders.1`

## Producers

```php
'producers' => [

    // Global middleware for all routes
    'middleware' => [
        // \KafkaBus\Commiter\Middleware\ProducerIdempotencyMiddleware::class,
    ],

    // Routes: message class → topic key
    'routes' => [
        App\Kafka\Messages\ProductMessage::class => 'products',
        App\Kafka\Messages\OrderMessage::class   => 'orders',
    ],

    'flush_timeout' => 5000, // ms
    'flush_retries' => 5,

    'additional_options' => [
        'compression.codec' => env('KAFKA_PRODUCER_COMPRESSION_CODEC', 'snappy'),
    ],
],
```

### Extended Route Format

For per-route individual settings, use an array instead of a string:

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

    // Global middleware for all workers
    'middleware' => [
        // \KafkaBus\Commiter\Middleware\ConsumerCommiterMiddleware::class,
    ],

    'workers' => [
        // --- Worker configuration options ---

        // 1. Single topic, worker name = topic key
        'products' => App\Kafka\Consumers\ProductsConsumer::class,

        // 2. Single topic with overrides
        'orders' => [
            'middleware' => [],
            'handler'    => App\Kafka\Consumers\OrdersConsumer::class,
        ],

        // 3. Single topic, worker name ≠ topic key
        'products-secondary' => [
            'topic_key'  => 'products',
            'middleware' => [],
            'handler'    => App\Kafka\Consumers\ProductsSecondaryConsumer::class,
        ],

        // 4. Multiple topics in one worker
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

    // Global consumer settings (overridable at the worker level)
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

### Worker Option Priority

Options are resolved in the following order (from lowest to highest priority):

```
global consumers.* → worker entry → per-topic (for multi-topic workers)
```

## All Environment Variables

| Variable                           | Default          | Description               |
|------------------------------------|------------------|---------------------------|
| `KAFKA_CONNECTION`                 | `kafka`          | Active connection         |
| `KAFKA_BROKER_LIST`                | `localhost:9092` | Broker addresses          |
| `KAFKA_PREFIX`                     | `{APP_ENV}.`     | Topic name prefix         |
| `KAFKA_SECURITY_PROTOCOL`          | `SASL_PLAINTEXT` | Security protocol         |
| `KAFKA_SASL_MECHANISMS`            | `PLAIN`          | SASL mechanism            |
| `KAFKA_SASL_USERNAME`              | —                | SASL username             |
| `KAFKA_SASL_PASSWORD`              | —                | SASL password             |
| `KAFKA_DEBUG`                      | `false`          | rdkafka debug logs        |
| `KAFKA_CONSUMER_AUTO_COMMIT`       | `false`          | Auto-commit offsets       |
| `KAFKA_CONSUMER_GROUP_ID`          | `{APP_NAME}`     | Consumer group            |
| `KAFKA_MAX_POLL_INTERVAL_MS`       | `300000`         | Max interval between polls|
| `KAFKA_SESSION_TIMEOUT_MS`         | `45000`          | Session timeout           |
| `KAFKA_HEARTBEAT_INTERVAL_MS`      | `3000`           | Heartbeat interval        |
| `KAFKA_PRODUCER_COMPRESSION_CODEC` | `snappy`         | Compression algorithm     |
