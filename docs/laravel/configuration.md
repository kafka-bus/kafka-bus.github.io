# Configuration

Configuration is split across two files: `config/kafka-bus.php` (connections, topics, producers, consumers) and `config/kafka-bus-commiter.php` (idempotency storage).

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

    // For tests — doesn't connect to a real broker
    'testing' => [
        'driver'  => 'null',
        'options' => [],
    ],
],
```

`default` is the name of the active connection. Switch at runtime with `KafkaBus::onConnection('testing')->publish(...)`.

### Drivers

| Driver  | Config class             | Behavior                                                 |
|----------|-----------------------------|---------------------------------------------------------------|
| `kafka` | `KafkaConnectionConfig`  | A real connection via rdkafka                                   |
| `null`  | `NullConnectionConfig`   | Publishes are discarded, reading throws an exception             |

The `debug` key is handled specially: it's extracted from `options` and passed as a separate flag (`true` → `debug => all`). Every other key goes to librdkafka as-is.

### Custom Driver

Instead of a driver name, you can specify a subclass of `DriverConnectionFactory` — it will be resolved through the container:

```php
'connections' => [
    'custom' => [
        'driver'  => App\Kafka\Drivers\MyDriverConnectionFactory::class,
        'options' => [/* ... */],
    ],
],
```

An unknown driver throws a `KafkaBusConfigurationException`.

## Topics

The physical topic name is built automatically from `topic_prefix` plus the value from `topics`:

```php
'topic_prefix' => env('KAFKA_PREFIX', env('APP_ENV', 'local') . '.'),

'topics' => [
    'products' => 'fact.products.1',
    'orders'   => 'fact.orders.1',
],
```

Final topic name = `topic_prefix` + the value from `topics`. With `APP_ENV=production`:
- `products` → `production.fact.products.1`
- `orders` → `production.fact.orders.1`

The key on the left (`products`) is the logical one — it's what routes, workers, and commands use.

::: warning
The prefix is concatenated with no separator. The trailing dot in `KAFKA_PREFIX` is required — otherwise you'd get `productionfact.products.1`.
:::

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

For per-route settings, use an array instead of a string:

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
In the extended format, the `topic_key` key is **required** — without it you'll get a `KafkaBusConfigurationException`. The short form `MessageClass::class => 'orders'` is equivalent to `['topic_key' => 'orders']`.
:::

Publishing a class that's missing from `routes` also throws a `KafkaBusConfigurationException` — when the route is built, not when the message is sent.

## Consumers

```php
'consumers' => [

    // Global middleware for all workers
    'middleware' => [
        // \KafkaBus\Commiter\Middleware\ConsumerCommiterMiddleware::class,
    ],

    'workers' => [
        // --- Worker configuration variants ---

        // 1. A single topic, worker name = topic key
        'products' => App\Kafka\Consumers\ProductsConsumer::class,

        // 2. A single topic with overrides
        'orders' => [
            'middleware' => [],
            'handler'    => App\Kafka\Consumers\OrdersConsumer::class,
        ],

        // 3. A single topic, worker name ≠ topic key
        'products-secondary' => [
            'topic_key'  => 'products',
            'middleware' => [],
            'handler'    => App\Kafka\Consumers\ProductsSecondaryConsumer::class,
        ],

        // 4. Multiple topics in one worker
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

    // Global consumer settings (overridden at the worker level)
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

How the worker format is detected:

| Value                             | Interpretation                                             |
|---------------------------------------|------------------------------------------------------------|
| string (class)                    | A single topic, topic key = worker name                       |
| array with a `handler` key        | A single topic; the key comes from `topic_key` or the name    |
| array with a `topics` key         | Multiple topics                                                |

### Settings Priority

`OptionsMerger` merges the global section with worker- or route-level settings:

```
global consumers.* / producers.*  →  worker / route settings
```

Scalar keys (`auto_commit`, `flush_timeout`, …) are overridden by the local value. Two keys are handled specially:

| Key                   | Merge behavior                                                   |
|-------------------------|----------------------------------------------------------------------|
| `middleware`          | Merged: global entries first (no duplicates), then local ones        |
| `additional_options`  | Merged by key, local values override global ones                     |

::: tip
Global middleware can't be disabled for an individual worker — the lists are combined, not replaced. If a middleware isn't needed everywhere, don't declare it globally — list it on specific workers instead.
:::

Middleware run in the order of the final list: global ones first. This works out well for `ConsumerCommiterMiddleware` — duplicate checking happens before the rest of the wrapping middleware.

### Filtering Additional Options

Parameters in `additional_options` pass through librdkafka's allow-list: `Options::PRODUCER_OPTIONS` for the producer, `Options::CONSUMER_OPTIONS` for the consumer. Keys not on the list are silently dropped. Full list — in [Core Configuration](/docs/configuration#filtering-producer-and-consumer-parameters).

Connection-level parameters (`metadata.broker.list`, SASL, `debug`) are set in the `connections` section and aren't subject to this filtering.

## Commiter

The `config/kafka-bus-commiter.php` file configures idempotency state storage.

```php
return [
    // The active source (a key from "sources")
    'source' => env('KAFKA_COMMITER_SOURCE', 'default'),

    'sources' => [
        'default' => [
            'driver' => 'database',
            'options' => [
                'connection' => env('KAFKA_COMMITER_CONNECTION'), // null = the default connection
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

    // For the kafka:commit:purge command (database only)
    'purge' => [
        'days' => env('KAFKA_COMMITER_PURGE_DAYS', 14),
    ],

    // Key-derivation strategy: "idempotency" | "native"
    'repository' => env('KAFKA_COMMITER_REPOSITORY', 'idempotency'),

    'repositories' => [
        'idempotency' => IdempotencyMessageRepository::class,
        'native'      => NativeMessageRepository::class,
    ],
];
```

### Storage Sources

| Driver     | Class                        | Notes                                                              |
|-------------|---------------------------------|------------------------------------------------------------------------|
| `database` | `DatabaseRepositorySource`  | The `kafka_bus_commits` table, `upsert`, supports `purge`                |
| `redis`    | `RedisRepositorySource`     | TTL hashes, atomic Lua scripts; cleanup via TTL, not `purge`             |

::: warning Redis requires PhpRedis
`RedisRepositorySource` only works with the `phpredis` driver — connecting via `predis` makes the provider throw an `InvalidArgumentException`. Key TTL is computed as `purge.days × 86400`.
:::

## All Environment Variables

### kafka-bus.php

| Variable                            | Default            | Description                       |
|-----------------------------------------|-----------------------|---------------------------------------|
| `KAFKA_CONNECTION`                  | `kafka`             | The active connection                    |
| `KAFKA_BROKER_LIST`                 | `localhost:9092`    | Broker addresses                          |
| `KAFKA_PREFIX`                      | `{APP_ENV}.`        | Topic name prefix                         |
| `KAFKA_SECURITY_PROTOCOL`           | `SASL_PLAINTEXT`    | Security protocol                          |
| `KAFKA_SASL_MECHANISMS`             | `PLAIN`             | SASL mechanism                             |
| `KAFKA_SASL_USERNAME`               | —                    | SASL username                              |
| `KAFKA_SASL_PASSWORD`               | —                    | SASL password                              |
| `KAFKA_DEBUG`                       | `false`              | rdkafka debug logs                         |
| `KAFKA_CONSUMER_AUTO_COMMIT`        | `false`              | Explicit commit after processing            |
| `KAFKA_CONSUMER_GROUP_ID`           | `{APP_NAME}`         | Consumer group                              |
| `KAFKA_MAX_POLL_INTERVAL_MS`        | `300000`             | Max interval between polls                  |
| `KAFKA_SESSION_TIMEOUT_MS`          | `45000`              | Session timeout                             |
| `KAFKA_HEARTBEAT_INTERVAL_MS`       | `3000`               | Heartbeat interval                          |
| `KAFKA_PRODUCER_COMPRESSION_CODEC`  | `snappy`             | Compression algorithm                       |

### kafka-bus-commiter.php

| Variable                       | Default          | Description                                    |
|-------------------------------------|---------------------|-----------------------------------------------------|
| `KAFKA_COMMITER_SOURCE`        | `default`         | The active storage source                          |
| `KAFKA_COMMITER_CONNECTION`    | —                  | The DB connection (`null` = default)               |
| `KAFKA_COMMITER_REPOSITORY`    | `idempotency`      | The key-derivation strategy                        |
| `KAFKA_COMMITER_PURGE_DAYS`    | `14`               | Record retention / Redis key TTL, in days           |
