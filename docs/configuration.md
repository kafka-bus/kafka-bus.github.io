# Configuration

The package is built around `ConnectionRegistry` — a registry of named connections to Kafka. Each connection is described by a `ConnectionConfigInterface` config object, while the connection object itself (`ConnectionInterface`) is created lazily by a driver from `DriverRegistry` on first access.

```
ConnectionRegistry
  ├── 'kafka'   → KafkaConnectionConfig ──[DriverRegistry]──▶ KafkaConnection
  └── 'testing' → NullConnectionConfig  ──[DriverRegistry]──▶ NullConnection
```

## Connections

### Connecting to Kafka

`KafkaConnectionConfig` is the configuration for a connection to a real broker:

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

Constructor parameters:

| Parameter    | Type                           | Default     | Description                                                      |
|--------------|----------------------------------|--------------|--------------------------------------------------------------------|
| `brokerList` | `string`                        | —            | The `metadata.broker.list` value, a comma-separated broker list   |
| `logLevel`   | `int`                            | `LOG_DEBUG`  | The `log_level` value for librdkafka                              |
| `debug`      | `bool`                           | `false`      | When `true`, enables `debug => all`                                |
| `saslConfig` | `?UserCredentialsConfig`        | `null`       | SASL credentials                                                   |
| `extra`      | `array<string, scalar\|null>`   | `[]`         | Any additional librdkafka parameters                               |

### Connecting with SASL Authentication

Credentials are supplied as a separate `UserCredentialsConfig` object rather than as raw keys:

```php
use KafkaBus\Core\Connections\Config\KafkaConnectionConfig;
use KafkaBus\Core\Connections\Config\UserCredentialsConfig;

$config = new KafkaConnectionConfig(
    brokerList: 'kafka.example.com:9092',
    saslConfig: new UserCredentialsConfig(
        username: 'my-user',
        password: 'my-password',
        protocol: 'SASL_SSL', // defaults to 'plaintext'
        mechanisms: 'SCRAM-SHA-512', // defaults to PLAIN
    ),
);
```

`UserCredentialsConfig::toOptions()` expands into four librdkafka parameters:

```php
[
    'security.protocol' => $protocol,
    'sasl.mechanisms'   => $mechanisms,
    'sasl.username'     => $username,
    'sasl.password'     => $password,
]
```

### Parameter Merge Order

`KafkaConnectionConfig::getOptions()` assembles the final set in this order (each subsequent group overrides the previous one):

```
base (metadata.broker.list, log_level, debug) → saslConfig → extra
```

### Multiple Connections

`ConnectionRegistry` supports several named connections — for example, for different clusters:

```php
$connectionRegistry = new ConnectionRegistry([
    'main'      => new KafkaConnectionConfig('kafka-main:9092'),
    'analytics' => new KafkaConnectionConfig('kafka-analytics:9092'),
]);
```

The default connection name isn't set on the registry — it's the second argument to `Bus`:

```php
$bus = new Bus($threadRegistry, defaultConnection: 'main');

// Publish through a non-default connection
$bus->onConnection('analytics')->publish($message);
```

Connections are created lazily and cached: a repeated lookup by the same name returns the same object. Looking up an unregistered name throws a `ConnectionException` listing the available connections.

### Null Driver

`NullConnection` accepts publishes and silently discards them via `NullProducer`. Creating a consumer on such a connection throws a `ConsumerException` — you can't read through it.

```php
use KafkaBus\Core\Connections\Config\NullConnectionConfig;

$connectionRegistry = new ConnectionRegistry([
    'testing' => new NullConnectionConfig(),
]);
```

::: tip
In the Laravel package, the `null` driver is configured via `'driver' => 'null'` in the config — you don't need a separate class.

For full tests that also need to *read* messages, use `ConnectionFaker` — see [Testing the Consumer](/docs/consumer/testing).
:::

### Quick Start Without Manual Wiring

For local development there's a ready-made registry with `default` and `null` connections:

```php
use KafkaBus\Core\Connections\Registry\ConnectionRegistry;

$connectionRegistry = ConnectionRegistry::default(); // 127.0.0.1:29092
$connectionRegistry = ConnectionRegistry::default('localhost', 9092);
```

### Custom Driver

`DriverRegistry` maps a config class to a connection factory. To add your own transport, register a "config class → factory" pair:

```php
use KafkaBus\Core\Connections\Registry\DriverRegistry;

$driverRegistry = new DriverRegistry(); // already includes kafka and null

$driverRegistry->add(
    MyConnectionConfig::class,
    static fn (string $name, MyConnectionConfig $config) => new MyConnection($name, $config),
);

$connectionRegistry = new ConnectionRegistry(
    ['custom' => new MyConnectionConfig()],
    $driverRegistry,
);
```

If no driver is registered for a config class, a `DriverException` is thrown listing the registered classes.

## Filtering Producer and Consumer Parameters

A key detail that's easy to forget: `KafkaConnectionConfig` connection parameters apply to the producer and consumer as a whole, **but additional route- or worker-level parameters are filtered through an allow-list**.

`Options::getProducerOptions()` and `Options::getConsumerOptions()` only pass through the keys from `additionalOptions` that appear in the corresponding list:

```php
// Bus\Publishers\Router\Options — reaches the producer
new Options(additionalOptions: ['compression.codec' => 'gzip']);

// Bus\Listeners\Workers\Options — silently dropped:
// 'compression.codec' isn't in CONSUMER_OPTIONS
new Options(additionalOptions: ['compression.codec' => 'gzip']);
```

::: warning
An unknown or "wrong-side" key doesn't raise an error — it's just dropped. If a parameter isn't taking effect, first check that it's in the right list.
:::

::: details Allowed producer keys (Options::PRODUCER_OPTIONS)
`transactional.id`, `transaction.timeout.ms`, `enable.idempotence`, `enable.gapless.guarantee`, `queue.buffering.max.messages`, `queue.buffering.max.kbytes`, `queue.buffering.max.ms`, `linger.ms`, `message.send.max.retries`, `retries`, `retry.backoff.ms`, `queue.buffering.backpressure.threshold`, `compression.codec`, `compression.type`, `batch.num.messages`, `batch.size`, `delivery.report.only.error`, `dr_cb`, `dr_msg_cb`, `sticky.partitioning.linger.ms`
:::

::: details Allowed consumer keys (Options::CONSUMER_OPTIONS)
`partition.assignment.strategy`, `session.timeout.ms`, `heartbeat.interval.ms`, `group.protocol.type`, `coordinator.query.interval.ms`, `max.poll.interval.ms`, `enable.auto.commit`, `auto.commit.interval.ms`, `enable.auto.offset.store`, `queued.min.messages`, `queued.max.messages.kbytes`, `fetch.wait.max.ms`, `fetch.message.max.bytes`, `max.partition.fetch.bytes`, `fetch.max.bytes`, `fetch.min.bytes`, `fetch.error.backoff.ms`, `offset.store.method`, `isolation.level`, `consume_cb`, `rebalance_cb`, `offset_commit_cb`, `enable.partition.eof`, `check.crcs`, `allow.auto.create.topics`, `auto.offset.reset`, `group.id`
:::

### Value Normalization

Before values are handed to librdkafka, booleans are cast to strings, and the resulting set is passed through `array_filter()` — falsy values are dropped:

| PHP value | What reaches librdkafka |
|-----------|--------------------------|
| `true`    | `'true'`                 |
| `false`   | `'false'`                |
| `null`    | dropped                  |
| `0`       | dropped                  |
| `'0'`     | dropped                  |
| `''`      | dropped                  |

Booleans are safe: `false` first becomes the non-empty string `'false'` and survives the filtering.

::: warning
A numeric `0`, however, is lost. Parameters like `'linger.ms' => 0` or `'queue.buffering.max.ms' => 0` will never reach the broker — pass them as a string instead: `'linger.ms' => '0'`.
:::

## Building the Bus

Once the connection registry is configured, you assemble `Bus` — the package's central facade:

```php
use KafkaBus\Core\Bus;

$bus = new Bus(
    new Bus\ThreadRegistry(
        $connectionRegistry,
        new Bus\ThreadFactory(
            new Bus\Listeners\ListenerFactory(workerRegistry: $workerRegistry),
            new Bus\Publishers\PublisherFactory(routes: $publisherRoutes),
        )
    ),
    defaultConnection: 'kafka'
);
```

What happens here:

- `ThreadRegistry` lazily creates one `Thread` per connection name and caches them;
- `ListenerFactory` can build a `Listener` by worker name from a `WorkerRegistryInterface`;
- `PublisherFactory` holds the `PublisherRoutes` table — shared across all connections;
- `Bus`'s constructor immediately resolves the `Thread` for `defaultConnection`.

## Parameter Reference

The most commonly used librdkafka parameters:

| Parameter               | Level        | Description                | Example                  |
|--------------------------|--------------|------------------------------|---------------------------|
| `metadata.broker.list`  | connection   | Broker addresses             | `kafka:9092,kafka2:9092`  |
| `security.protocol`     | connection   | Security protocol            | `SASL_PLAINTEXT`, `SSL`   |
| `sasl.mechanisms`       | connection   | SASL mechanism                | `PLAIN`, `SCRAM-SHA-256`  |
| `debug`                 | connection   | rdkafka debug logs            | `consumer,cgrp,topic`     |
| `compression.codec`     | producer     | Compression                   | `snappy`, `gzip`, `lz4`   |
| `linger.ms`             | producer     | Delay before batching         | `10`                      |
| `group.id`              | consumer     | Consumer group                | `my-service`              |
| `auto.offset.reset`     | consumer     | Starting position             | `earliest`, `latest`      |
| `max.poll.interval.ms`  | consumer     | Poll timeout                  | `300000`                  |
| `session.timeout.ms`    | consumer     | Session timeout               | `45000`                   |

Full list — in the [librdkafka documentation](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).
