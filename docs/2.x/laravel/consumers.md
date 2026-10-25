---
title: Consuming messages in Laravel
description: Writing handlers, registering consumer routes, running workers with Artisan, and deploying them under Supervisor
---

# Consuming messages

In a Laravel application, consuming has three steps: write a handler class, bind it to a topic in `consumers.routes`, and run a worker with `php artisan kafka:consume`. This page walks through each step and the common setups.

## Creating a handler

Create a class in `app/Kafka/Consumers/`. The type of the `__invoke` argument determines how the message is delivered:

```php
// app/Kafka/Consumers/ProductsConsumer.php

namespace App\Kafka\Consumers;

use KafkaBus\Core\Consumers\Messages\ConsumerMessageInterface;

class ProductsConsumer
{
    public function __invoke(ConsumerMessageInterface $message): void
    {
        $payload = json_decode($message->payload(), true);

        Product::updateOrCreate(
            ['id' => $payload['id']],
            ['name' => $payload['name'], 'status' => $payload['status']],
        );
    }
}
```

The handler is resolved through the Laravel container, so dependencies can be injected via the constructor:

```php
class ProductsConsumer
{
    public function __construct(
        private readonly ProductRepository $products,
        private readonly LoggerInterface $logger,
    ) {
    }

    public function __invoke(ConsumerMessageInterface $message): void
    {
        // ...
    }
}
```

### Other argument types

```php
// Payload as a plain string
public function __invoke(string $payload): void

// Payload as a decoded JSON array
public function __invoke(array $data): void

// The original rdkafka object (offset, partition, key)
public function __invoke(\RdKafka\Message $message): void
```

More on type detection — in [Consuming messages](/docs/2.x/consumer/consuming#factory-resolution-order).

## Registering a route

Bind the handler to a topic key in `consumers.routes`:

```php
// config/kafka-bus.php
'topics' => [
    'products' => 'fact.products.1',
],

'consumers' => [
    'routes' => [
        'products' => App\Kafka\Consumers\ProductsConsumer::class,
    ],
],
```

Each topic has one handler. Route formats and options — in [Configuration](/docs/2.x/laravel/configuration#routes).

## Typed handler

With `kafka-bus/messages`, add the `#[MessageFactory]` attribute:

```php
use App\Kafka\Messages\ProductMessage;
use KafkaBus\Core\Receivers\Attributes\MessageFactory;
use KafkaBus\Messages\DomainEventEnum;
use KafkaBus\Messages\Factories\DomainMessageFactory;

class ProductsConsumer
{
    #[MessageFactory(new DomainMessageFactory(ProductMessage::class))]
    public function __invoke(ProductMessage $message): void
    {
        match ($message->getEvent()) {
            DomainEventEnum::Create => $this->onCreate($message),
            DomainEventEnum::Update => $this->onUpdate($message),
            DomainEventEnum::Delete => $this->onDelete($message),
        };
    }

    private function onCreate(ProductMessage $message): void
    {
        Product::create(['id' => $message->id, 'name' => $message->name]);
    }

    private function onUpdate(ProductMessage $message): void
    {
        Product::find($message->id)?->update(
            collect($message->getDirty())
                ->mapWithKeys(fn (string $field) => [$field => $message->$field])
                ->all()
        );
    }

    private function onDelete(ProductMessage $message): void
    {
        Product::find($message->id)?->delete();
    }
}
```

::: warning
The event and the list of changed fields are available via the `getEvent()` and `getDirty()` methods, not as properties. `$message->event` returns `null`, because it goes through `Payload`'s magic getter.
:::

## Starting a worker

Every route is also a worker with the same name, so a single topic can be read right away:

```bash
php artisan kafka:consume products
```

The command starts the blocking read loop and handles `SIGTERM`, `SIGINT`, and `SIGQUIT`: the current message is finished and committed, then the process exits.

### Several topics in one process

Declare a worker that lists the topics:

```php
'consumers' => [
    'routes' => [
        'products' => App\Kafka\Consumers\ProductsConsumer::class,
        'orders'   => App\Kafka\Consumers\OrdersConsumer::class,
    ],

    'workers' => [
        'default' => [
            'topics' => ['products', 'orders'],
        ],
    ],
],
```

```bash
php artisan kafka:consume default
```

You can also pass several worker names — their topics are merged into one consumer:

```bash
php artisan kafka:consume products orders
```

When merging, consumer options (`group.id`, `auto_commit`, `consumer_timeout`) come from the first worker in the list. Merge only workers with compatible settings — see [Workers](/docs/2.x/consumer/workers#several-workers-in-one-process).

All topics of one process are read by a single consumer in a single group — messages are processed sequentially, with no ordering guarantee between topics.

### All routes at once

```bash
php artisan kafka:consume:routes
php artisan kafka:consume:routes --expect=orders
```

The command reads every topic from `consumers.routes` in one process with the global consumer options; `--expect` excludes topic keys. Handy for local development and small services.

## Worker options

A worker can override the global consumer settings:

```php
'workers' => [
    'orders-slow' => [
        'topics'             => ['orders'],
        'auto_commit'        => true,
        'consumer_timeout'   => 20_000,
        'additional_options' => [
            'max.poll.interval.ms' => 900_000,
        ],
    ],
],
```

A worker's `additional_options` are merged with the global ones by key, so only the listed keys are overridden.

## Middleware

Global middleware from `consumers.middleware` is added to every route. Route-specific middleware goes into the route:

```php
'consumers' => [
    'middleware' => [
        KafkaBus\Commiter\Middleware\ConsumerCommiterMiddleware::class,
    ],

    'routes' => [
        'orders' => [
            'handler'    => App\Kafka\Consumers\OrdersConsumer::class,
            'middleware' => [App\Kafka\Middleware\TenantMiddleware::class],
        ],
    ],
],
```

Middleware are specified as classes and resolved through the container. They implement `ReceiverPipelineMiddleware` and see both the raw message (`target()`) and the deserialized one (`formatted()`). See [Consumer pipeline](/docs/2.x/consumer/pipeline).

::: info Changed in 2.0
Workers no longer have their own `middleware`. Middleware belongs to routes, so it applies to a topic no matter which worker reads it.
:::

## One topic, several consumer groups

A topic has one handler per application. To read the same topic in two independent ways, run two workers with different `group.id` — both will dispatch to the same handler:

```php
'workers' => [
    'products' => [
        'topics' => ['products'],
    ],

    'products-replay' => [
        'topics'             => ['products'],
        'additional_options' => ['group.id' => env('APP_NAME').'-replay'],
    ],
],
```

If the processing logic must differ, branch inside the handler or move the second consumer into a separate application.

::: warning
Workers reading the same topic with the same `group.id` share its partitions — each message reaches only one of them.
:::

## Supervisor configuration

```ini
[program:kafka-products]
command=php /var/www/artisan kafka:consume products
directory=/var/www
autostart=true
autorestart=true
stopwaitsecs=60
stopsignal=TERM
user=www-data
numprocs=1
stdout_logfile=/var/www/storage/logs/kafka-products.log
stderr_logfile=/var/www/storage/logs/kafka-products-err.log
```

::: tip
`stopwaitsecs` should exceed `consumer_timeout`, or Supervisor kills the process before it finishes its current iteration.

Increasing `numprocs` only helps up to the number of partitions of the topic: extra processes in the same group get no partitions.
:::

::: warning If the handler fails, so does the process
An uncaught exception from the handler stops the worker, and Supervisor restarts it. Without protection, a "poison" message causes a restart loop.

Add a middleware with `try/catch` ([example](/docs/2.x/consumer/pipeline#middleware-with-error-handling)) or `ConsumerCommiterMiddleware` with a `maxAttempt` limit.
:::

## Inspecting the configuration

```bash
php artisan kafka:consumer:route:list
php artisan kafka:worker:list
```

The first command prints every topic with its handler and the final middleware chain, the second — every worker and its topics. See [Commands](/docs/2.x/laravel/commands).
