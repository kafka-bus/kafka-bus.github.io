# Конфигурация

Пакет строится вокруг `ConnectionRegistry` — реестра именованных подключений к Kafka. Каждое подключение реализует `ConnectionInterface` и создаётся через соответствующий драйвер.

## Подключения

### Kafka Connection (основной драйвер)

`KafkaConnection` использует `ext-rdkafka` и принимает любые параметры `librdkafka` в массиве `options`:

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

### Подключение с SASL аутентификацией

```php
$config = new KafkaConnectionConfig(options: [
    'metadata.broker.list' => 'kafka.example.com:9092',
    'security.protocol'    => 'SASL_PLAINTEXT',
    'sasl.mechanisms'      => 'PLAIN',
    'sasl.username'        => 'my-user',
    'sasl.password'        => 'my-password',
]);
```

### Множественные подключения

`ConnectionRegistry` поддерживает несколько именованных подключений — например, для разных кластеров:

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

Переключение подключения на уровне Bus:

```php
// Публикация через нестандартное подключение
$bus->onConnection('analytics')->publish($message);
```

### Null Driver (для тестов)

`NullConnection` принимает вызовы, но не взаимодействует с реальным брокером. Используйте в тестовом окружении:

```php
use KafkaBus\Core\Connections\NullConnection;

$connectionRegistry = new ConnectionRegistry(
    driverRegistry: $driverRegistry,
    connections: ['testing' => new NullConnectionConfig()],
    defaultConnectionName: 'testing',
);
```

::: tip
В пакете для Laravel драйвер `null` настраивается через `'driver' => 'null'` в конфиге — создавать отдельный класс не нужно.
:::

## Сборка Bus

После настройки реестра подключений собирается `Bus` — центральный фасад пакета:

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

## Справочник параметров

Наиболее часто используемые параметры:

| Параметр               | Описание                  | Пример                   |
|------------------------|---------------------------|--------------------------|
| `metadata.broker.list` | Адреса брокеров           | `kafka:9092,kafka2:9092` |
| `security.protocol`    | Протокол безопасности     | `SASL_PLAINTEXT`, `SSL`  |
| `sasl.mechanisms`      | Механизм SASL             | `PLAIN`, `SCRAM-SHA-256` |
| `debug`                | Отладочные логи rdkafka   | `consumer,cgrp,topic`    |
| `compression.codec`    | Сжатие (продьюсер)        | `snappy`, `gzip`, `lz4`  |
| `group.id`             | Группа потребителей       | `my-service`             |
| `auto.offset.reset`    | Начальная позиция         | `earliest`, `latest`     |
| `max.poll.interval.ms` | Таймаут опроса            | `300000`                 |

Полный список — в [документации librdkafka](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).
