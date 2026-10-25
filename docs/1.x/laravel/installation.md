# Installation

## Requirements

| Dependency    | Version                                    |
|-----------------|----------------------------------------------|
| PHP           | `^8.2`                                      |
| Laravel       | `^10.0 \|\| ^11.0 \|\| ^12.0 \|\| ^13.0`   |
| `ext-rdkafka` | any current                                  |

The package pulls in `kafka-bus/core`, `kafka-bus/commiter`, and `kafka-bus/messages` — you don't need to install them separately.

## Installation

```bash
composer require kafka-bus/laravel-bridge
```

The package uses auto-discovery. **Two** providers get registered:

| Provider                     | What it registers                                              |
|---------------------------------|--------------------------------------------------------------------|
| `KafkaBusServiceProvider`   | `BusInterface`, connection and topic registries, Artisan commands   |
| `CommiterServiceProvider`   | `RepositorySourceInterface`, `ConsumerMessageRepositoryInterface`    |

## Publishing the Config

```bash
php artisan vendor:publish --tag=kafka-bus
```

This creates `config/kafka-bus.php` with the full configuration for connections, topics, producers, and consumers.

::: tip
Publishing the config is optional: the provider merges default values via `mergeConfigFrom()`. But without publishing it you can't define topics and routes, so in practice this is the first step.
:::

## Setting Up .env

The minimum set of variables to connect to a broker:

```dotenv
# Broker address
KAFKA_BROKER_LIST=localhost:9092

# Topic prefix (defaults to APP_ENV + '.')
KAFKA_PREFIX=production.

# Consumer group
KAFKA_CONSUMER_GROUP_ID=my-service

# Connection (defaults to 'kafka')
KAFKA_CONNECTION=kafka
```

::: warning Broker Without Authentication
The default config sets `KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT`. For a local broker with no SASL, override the protocol, or the connection won't be established:

```dotenv
KAFKA_SECURITY_PROTOCOL=PLAINTEXT
```
:::

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
- a migration for the `kafka_bus_commits` table (`key`, `number`, `commited_at` columns)

Enable the middleware in the config:

```php
// config/kafka-bus.php
'consumers' => [
    'middleware' => [
        # Can also be set at the worker level if not every topic needs a commit
        KafkaBus\Commiter\Middleware\ConsumerCommiterMiddleware::class,
    ],
],

'producers' => [
    'middleware' => [
        KafkaBus\Commiter\Middleware\ProducerIdempotencyMiddleware::class,
    ],
],
```

Middleware are resolved through the Laravel container, so `ConsumerCommiterMiddleware` automatically receives the repository configured by `CommiterServiceProvider`.

More details — in [Commiter](/docs/1.x/components/commiter) and [Configuration](/docs/1.x/laravel/configuration#commiter).

## Octane Support

If `laravel/octane` is installed, the provider subscribes to the `WorkerStarting` event and warms up `BusInterface` when the worker starts. No extra configuration is needed.

The `laravel/octane` package is listed in `suggest` — there's no need to install it just for kafka-bus.

## Verifying the Installation

```bash
# List registered workers
php artisan kafka:worker:list

# List producer routes
php artisan kafka:route:list
```

Both commands read the config and resolve handlers through the container, so a typo in a class name shows up immediately.

## What's Next

- [Configuration](/docs/1.x/laravel/configuration) — a full walkthrough of `config/kafka-bus.php`
- [Producing Messages](/docs/1.x/laravel/producers) — publishing via the Facade and DI
- [Consuming Messages](/docs/1.x/laravel/consumers) — creating workers and handlers
- [Artisan Commands](/docs/1.x/laravel/commands) — managing workers and offsets
