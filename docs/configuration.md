# Конфигурация

Пакет строится вокруг `ConnectionRegistry` — реестра именованных соединений с Kafka. Каждое соединение реализует `ConnectionInterface` и создаётся через соответствующий драйвер.

## Соединения

### Kafka-соединение (основной драйвер)

`KafkaConnection` использует `ext-rdkafka` и принимает любые опции `librdkafka` в массиве `options`:

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

### Подключение с SASL-аутентификацией

```php
$config = new KafkaConnectionConfig(options: [
    'metadata.broker.list' => 'kafka.example.com:9092',
    'security.protocol'    => 'SASL_PLAINTEXT',
    'sasl.mechanisms'      => 'PLAIN',
    'sasl.username'        => 'my-user',
    'sasl.password'        => 'my-password',
]);
```

### Несколько соединений

`ConnectionRegistry` поддерживает несколько именованных соединений — например, для разных кластеров:

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

Переключение соединения на уровне Bus:

```php
// Опубликовать через не-дефолтное соединение
$bus->onConnection('analytics')->publish($message);
```

### Null-драйвер (для тестов)

`NullConnection` принимает вызовы, но не взаимодействует с реальным брокером. Используйте его в тестовом окружении:

```php
use KafkaBus\Core\Connections\NullConnection;

// Вместо реального соединения:
$connectionRegistry = new ConnectionRegistry(
    driverRegistry: $driverRegistry,
    connections: ['testing' => new NullConnectionConfig()],
    defaultConnectionName: 'testing',
);
```

::: tip
В Laravel-пакете `null`-драйвер настраивается через `'driver' => 'null'` в конфиге — отдельный класс создавать не нужно.
:::

## Сборка Bus

После настройки реестра соединений собирается `Bus` — центральный фасад пакета:

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

## Справочник опций

Наиболее часто используемые опции:

| Опция                  | Описание                | Пример                   |
|------------------------|-------------------------|--------------------------|
| `metadata.broker.list` | Адреса брокеров         | `kafka:9092,kafka2:9092` |
| `security.protocol`    | Протокол безопасности   | `SASL_PLAINTEXT`, `SSL`  |
| `sasl.mechanisms`      | Механизм SASL           | `PLAIN`, `SCRAM-SHA-256` |
| `debug`                | Отладочные логи rdkafka | `consumer,cgrp,topic`    |
| `compression.codec`    | Сжатие (для producer)   | `snappy`, `gzip`, `lz4`  |
| `group.id`             | Consumer group          | `my-service`             |
| `auto.offset.reset`    | Стартовая позиция       | `earliest`, `latest`     |
| `max.poll.interval.ms` | Таймаут поллинга        | `300000`                 |

Полный список — в [документации librdkafka](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).
