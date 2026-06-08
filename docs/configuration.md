# Configuration

The package is built around `ConnectionRegistry` — a registry of named connections to Kafka. Each connection implements `ConnectionInterface` and is created via the corresponding driver.

## Connections

### Kafka Connection (main driver)

`KafkaConnection` uses `ext-rdkafka` and accepts any `librdkafka` options in the `options` array:

```php
use KafkaBus\Core\Connections\Config\KafkaConnectionConfig;
use KafkaBus\Core\Connections\Registry\ConnectionRegistry;
use KafkaBus\Core\Connections\Registry\DriverRegistry;

$config = new KafkaConnectionConfig(options: [
    'metadata.broker.list' => 'localhost:9092',
]);

$driverRegistry = new DriverRegistry();

$connectionRegistry = new ConnectionRegistry(
    driverRegistry: $driverRegistry,
    connections: ['kafka' => $config],
    defaultConnectionName: 'kafka',
);
```

### Connection with SASL Authentication

```php
$config = new KafkaConnectionConfig(options: [
    'metadata.broker.list' => 'kafka.example.com:9092',
    'security.protocol'    => 'SASL_PLAINTEXT',
    'sasl.mechanisms'      => 'PLAIN',
    'sasl.username'        => 'my-user',
    'sasl.password'        => 'my-password',
]);
```

### Multiple Connections

`ConnectionRegistry` supports multiple named connections — for example, for different clusters:

```php
$connectionRegistry = new ConnectionRegistry(
    driverRegistry: $driverRegistry,
    connections: [
        'main'     => new KafkaConnectionConfig(['metadata.broker.list' => 'kafka-main:9092']),
        'analytics'=> new KafkaConnectionConfig(['metadata.broker.list' => 'kafka-analytics:9092']),
    ],
    defaultConnectionName: 'main',
);
```

Switching connections at the Bus level:

```php
// Publish via a non-default connection
$bus->onConnection('analytics')->publish($message);
```

### Null Driver (for tests)

`NullConnection` accepts calls but does not interact with a real broker. Use it in your test environment:

```php
use KafkaBus\Core\Connections\NullConnection;

// Instead of a real connection:
$connectionRegistry = new ConnectionRegistry(
    driverRegistry: $driverRegistry,
    connections: ['testing' => new NullConnectionConfig()],
    defaultConnectionName: 'testing',
);
```

::: tip
In the Laravel package, the `null` driver is configured via `'driver' => 'null'` in the config — no need to create a separate class.
:::

## Building the Bus

After configuring the connection registry, the `Bus` — the package's central facade — is assembled:

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
    defaultConnectionName: 'kafka'
);
```

## Options Reference

The most commonly used options:

| Option                 | Description              | Example                  |
|------------------------|--------------------------|--------------------------|
| `metadata.broker.list` | Broker addresses         | `kafka:9092,kafka2:9092` |
| `security.protocol`    | Security protocol        | `SASL_PLAINTEXT`, `SSL`  |
| `sasl.mechanisms`      | SASL mechanism           | `PLAIN`, `SCRAM-SHA-256` |
| `debug`                | rdkafka debug logs       | `consumer,cgrp,topic`    |
| `compression.codec`    | Compression (producer)   | `snappy`, `gzip`, `lz4`  |
| `group.id`             | Consumer group           | `my-service`             |
| `auto.offset.reset`    | Start position           | `earliest`, `latest`     |
| `max.poll.interval.ms` | Poll timeout             | `300000`                 |

Full list — in the [librdkafka documentation](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).
