# Installation
## Requirements

| Dependency    | Version                        |
|---------------|-------------------------------|
| PHP           | `^8.2`                        |
| Laravel       | `^10.0 \|\| ^11.0 \|\| ^12.0` |
| `ext-rdkafka` | any current                   |

## Installation

```bash
composer require kafka-bus/laravel-bridge
```

The package uses auto-discovery — `KafkaBusServiceProvider` is registered automatically.

## Publishing the Config

```bash
php artisan vendor:publish --tag=kafka-bus
```

This creates `config/kafka-bus.php` with a full configuration for connections, topics, producers, and consumers.

## Configuring .env

Minimum set of variables for connecting to the broker:

```dotenv
# Broker address
KAFKA_BROKER_LIST=localhost:9092

# Topic prefix (default = APP_ENV + '.')
KAFKA_PREFIX=production.

# Consumer group
KAFKA_CONSUMER_GROUP_ID=my-service

# Connection (default 'kafka')
KAFKA_CONNECTION=kafka
```

### SASL Authentication

```dotenv
KAFKA_BROKER_LIST=pkc-xxx.us-east-1.aws.confluent.cloud:9092
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISMS=PLAIN
KAFKA_SASL_USERNAME=your-api-key
KAFKA_SASL_PASSWORD=your-api-secret
```

## Installing Commiter

If you need idempotent message processing, publish the Commiter config and migrations:

```bash
php artisan vendor:publish --tag=kafka-bus-commiter
php artisan migrate
```

This creates:
- `config/kafka-bus-commiter.php`
- Migration for the `kafka_bus_commits` table

Enable the middleware in the config:

```php
// config/kafka-bus.php
'consumers' => [
    'middleware' => [
        # Can be set at the worker level if commits are not needed for all topics
        KafkaBus\Commiter\Middleware\ConsumerCommiterMiddleware::class,
    ],
],

'producers' => [
    'middleware' => [
        KafkaBus\Commiter\Middleware\ProducerIdempotencyMiddleware::class,
    ],
],
```

Learn more in the [Commiter](/docs/components/commiter) section.

## Verifying the Installation

```bash
# List registered workers
php artisan kafka:worker:list

# List producer routes
php artisan kafka:route:list
```

## What's Next

- [Configuration](/docs/laravel/configuration) — full breakdown of `config/kafka-bus.php`
- [Producer](/docs/laravel/producers) — publishing messages via Facade and DI
- [Consumer](/docs/laravel/consumers) — creating workers and handlers
- [Artisan Commands](/docs/laravel/commands) — managing workers and offsets
