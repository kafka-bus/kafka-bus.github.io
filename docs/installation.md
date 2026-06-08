# Installation

## Requirements

| Dependency           | Version          | Required      |
|----------------------|------------------|---------------|
| PHP                  | `^8.2`           | ✅             |
| `ext-rdkafka`        | any current      | ✅             |
| Apache Kafka cluster | `2.x+`           | ✅             |
| `ext-pcntl`          | any              | Recommended   |

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

## Package Installation

```bash
composer require kafka-bus/core
```

## Verification

```bash
php -m | grep rdkafka
# rdkafka

php -r "echo \RdKafka\Conf::class;"
# RdKafka\Conf
```

## Additional Packages

Depending on your stack, you can install integration packages:

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
- [Topic Registration](/docs/topics)
- [Usage Examples](https://github.com/kafka-bus/kafka-bus/tree/1.x/examples)
