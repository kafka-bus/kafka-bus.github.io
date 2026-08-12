# Installation

## Requirements

| Dependency            | Version           | Required      |
|------------------------|--------------------|----------------|
| PHP                    | `^8.2`             | ✅             |
| `ext-rdkafka`          | any current        | ✅             |
| Apache Kafka cluster   | `2.x+`             | ✅             |
| `ext-pcntl`            | any                | Recommended    |

`ext-rdkafka` is a hard requirement of `kafka-bus/core`: all low-level I/O goes through `RdKafka\Producer` and `RdKafka\KafkaConsumer`.

`ext-pcntl` isn't strictly required, but without it you can't cleanly stop a long-running consumer process on `SIGTERM`/`SIGINT` — see [Graceful Shutdown](/docs/consumer/consuming#graceful-shutdown-via-signals).

::: details How to install ext-rdkafka
```bash
# Ubuntu / Debian
sudo apt-get install librdkafka-dev
pecl install rdkafka
echo "extension=rdkafka.so" >> /etc/php/8.2/cli/php.ini

# macOS (Homebrew)
brew install librdkafka
pecl install rdkafka

# Docker (official php image)
RUN apt-get install -y librdkafka-dev \
    && pecl install rdkafka \
    && docker-php-ext-enable rdkafka
```
:::

## Installing the Package

```bash
composer require kafka-bus/core
```

## Verify

```bash
php -m | grep rdkafka
# rdkafka

php -r "echo \RdKafka\Conf::class;"
# RdKafka\Conf
```

## Ecosystem Overview

| Package                     | Namespace           | Purpose                                                            |
|------------------------------|----------------------|----------------------------------------------------------------------|
| `kafka-bus/core`             | `KafkaBus\Core`     | Bus, connections, routing, producer and consumer pipeline           |
| `kafka-bus/messages`         | `KafkaBus\Messages` | Typed `Payload`, casters, `DomainMessage`, test factories           |
| `kafka-bus/commiter`         | `KafkaBus\Commiter` | Idempotent processing: consumer and producer middleware              |
| `kafka-bus/laravel-bridge`   | `KafkaBus\Laravel`  | Laravel integration: config, facade, Artisan commands, `FakeBus`    |

## Optional Packages

Install the integration package that matches your stack:

::: code-group

```bash [Laravel]
composer require kafka-bus/laravel-bridge
```

```bash [Spiral]
composer require kafka-bus/spiral-bridge # In development
```

:::

For typed messages:

```bash
composer require kafka-bus/messages
```

For idempotent processing:

```bash
composer require kafka-bus/commiter
```

For the Outbox pattern (in development):

```bash
composer require kafka-bus/outbox
```

## What's Next

- [Connection Configuration](/docs/configuration)
- [Registering Topics](/docs/topics)
- [Usage Examples](https://github.com/kafka-bus/kafka-bus/tree/1.x/examples)
