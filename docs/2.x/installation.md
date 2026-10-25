---
title: Installation
description: Requirements and installation of Kafka Bus 2.x — the meta-package and individual components
---

# Installation

Kafka Bus is a PHP client for Apache Kafka built on top of `ext-rdkafka`. This page covers the requirements, the one-line installation, and which individual packages to pick if you don't need the whole set.

This documentation describes Kafka Bus **2.x**. If you're upgrading from 1.x, see [Upgrading from 1.x](/docs/2.x/upgrade).

## Requirements

| Dependency | Version | Required |
|---|---|---|
| PHP | `^8.2` | ✅ |
| `ext-rdkafka` | any current | ✅ |
| Apache Kafka cluster | `2.x+` | ✅ |
| `ext-pcntl` | any | Recommended |

`ext-rdkafka` is required by every package: all low-level I/O goes through `RdKafka\Producer` and `RdKafka\KafkaConsumer`.

`ext-pcntl` isn't strictly required, but without it you can't cleanly stop a long-running worker on `SIGTERM`/`SIGINT` — see [Graceful shutdown](/docs/2.x/consumer/workers#graceful-shutdown).

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

## Installing the package

Install the meta-package — it ships every component of the tool:

```bash
composer require kafka-bus/kafka-bus
```

Using Laravel? Install the bridge instead — it already pulls in `kafka-bus/kafka-bus`:

```bash
composer require kafka-bus/laravel-bridge
```

See [Laravel installation](/docs/2.x/laravel/installation).

## Verify

```bash
php -m | grep rdkafka
# rdkafka

php -r "echo \RdKafka\Conf::class;"
# RdKafka\Conf
```

## Installing individual components

The meta-package `kafka-bus/kafka-bus` replaces five packages. You can install only the ones you need — all of them are released together under the same version tag.

| Package | Namespace | Purpose | Depends on |
|---|---|---|---|
| `kafka-bus/core` | `KafkaBus\Core` | `Bus`, connections, topics, routing, publish and receive pipelines | — |
| `kafka-bus/worker` | `KafkaBus\Worker` | Polling loop that reads Kafka and hands messages to `Bus` | `core` |
| `kafka-bus/metadata` | `KafkaBus\Metadata` | Topic and partition listing, consumer group offsets | `core` |
| `kafka-bus/messages` | `KafkaBus\Messages` | Typed `Payload`, casters, `DomainMessage`, test factories | `core` |
| `kafka-bus/commiter` | `KafkaBus\Commiter` | Idempotent processing middleware | `core`, `psr/log` |
| `kafka-bus/laravel-bridge` | `KafkaBus\Laravel` | Laravel integration: config, facade, Artisan commands, `FakeBus` | `kafka-bus/kafka-bus` |

Typical sets:

| Scenario | Packages |
|---|---|
| Service only publishes messages | `kafka-bus/core` |
| Service reads topics | `kafka-bus/core`, `kafka-bus/worker` |
| Viewing and resetting offsets | add `kafka-bus/metadata` |
| Typed messages | add `kafka-bus/messages` |
| Duplicate protection | add `kafka-bus/commiter` |

```bash
composer require kafka-bus/core kafka-bus/worker
```

## What's next

- [Connection configuration](/docs/2.x/configuration)
- [Architecture](/docs/2.x/architecture)
- [Registering topics](/docs/2.x/topics)
- [Usage examples](https://github.com/kafka-bus/kafka-bus/tree/2.x/examples)
