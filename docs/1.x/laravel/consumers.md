# Consuming Messages

## Creating a Handler

Create a class in `app/Kafka/Consumers/`. The type of the `__invoke` argument determines how the message is delivered:

```php
// app/Kafka/Consumers/ProductsConsumer.php

namespace App\Kafka\Consumers;

use KafkaBus\Core\Interfaces\Consumers\Messages\ConsumerMessageInterface;

class ProductsConsumer
{
    public function __invoke(ConsumerMessageInterface $message): void
    {
        $payload = json_decode($message->payload(), true);
        $headers = $message->headers();

        // Business logic
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
    ) {}

    public function __invoke(ConsumerMessageInterface $message): void { /* ... */ }
}
```

### Other Argument Type Options

```php
// Payload as a plain string
public function __invoke(string $payload): void { ... }

// Payload as a decoded JSON array
public function __invoke(array $data): void { ... }

// The original rdkafka object (offset, partition, key)
public function __invoke(\RdKafka\Message $message): void { ... }
```

More on the auto-detection mechanism — in [Consuming Messages](/docs/1.x/consumer/consuming#factory-resolution-order).

## Registering in the Config

```php
// config/kafka-bus.php
'consumers' => [
    'workers' => [
        'products' => App\Kafka\Consumers\ProductsConsumer::class,
    ],
],
```

Here the worker name matches the topic key. Other formats — in [Configuration](/docs/1.x/laravel/configuration#consumers).

## Typed Handler

When using `kafka-bus/messages`, add the `#[MessageFactory]` attribute:

```php
use App\Kafka\Messages\ProductMessage;
use KafkaBus\Core\Consumers\Attributes\MessageFactory;
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

## Starting a Worker

```bash
php artisan kafka:consume products
```

This command starts the blocking read loop and correctly handles `SIGTERM`, `SIGINT`, and `SIGQUIT`: the current message is finished and committed, and then the process exits.

### Multiple Workers in One Process

The command accepts a list of names:

```bash
php artisan kafka:consume products orders
```

Workers are merged, but connection settings (`group.id`, `auto_commit`, `consumer_timeout`) are taken from the **first** worker in the list. Only merge workers with compatible settings — more details in [Architecture](/docs/1.x/architecture#multiple-workers-in-one-process).

### All Workers at Once

```bash
php artisan kafka:consume:all
php artisan kafka:consume:all --expect=products-analytics
```

### Supervisor Configuration

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
`stopwaitsecs` should exceed `consumer_timeout`, or Supervisor will kill the process before it finishes its current iteration.

Increasing `numprocs` only makes sense up to the number of partitions on the topic: extra processes in the same group end up with no partitions assigned.
:::

::: warning If the Handler Crashes, So Does the Process
An uncaught exception from the handler stops the worker, and Supervisor restarts it. Without protection, a "poison" message causes a restart loop.

Add middleware with `try/catch` ([example](/docs/1.x/consumer/pipeline#middleware-with-error-handling)) or `ConsumerCommiterMiddleware` with a `maxAttempt` limit.
:::

## A Worker with Multiple Topics

A single worker can listen to several topics at once:

```php
'workers' => [
    'default' => [
        'topics' => [
            'products' => App\Kafka\Consumers\ProductsConsumer::class,
            'orders'   => App\Kafka\Consumers\OrdersConsumer::class,
        ],
    ],
],
```

```bash
php artisan kafka:consume default
```

All topics are read by a single consumer in a single group — messages are processed sequentially, with no ordering guarantee between topics.

## Worker-Level Middleware

```php
'workers' => [
    'products' => [
        'middleware' => [
            App\Kafka\Middleware\LoggingMiddleware::class,
            KafkaBus\Commiter\Middleware\ConsumerCommiterMiddleware::class,
        ],
        'handler' => App\Kafka\Consumers\ProductsConsumer::class,
    ],
],
```

Middleware are specified as classes and resolved through the container, so you can inject dependencies into them. The global middleware from `consumers.middleware` is added to this list and runs first.

## Topic-Level Middleware

```php
'workers' => [
    'default' => [
        'topics' => [
            'orders' => [
                'handler'    => App\Kafka\Consumers\OrdersConsumer::class,
                'middleware' => [App\Kafka\Middleware\TenantMiddleware::class],
            ],
        ],
    ],
],
```

Topic middleware implements `MessagePipelineMiddleware` and sees the already-deserialized object, while worker middleware implements `ConsumerPipelineMiddleware` and sees the raw message. See [Pipeline](/docs/1.x/consumer/pipeline).

## Multiple Workers for One Topic

For when you need different processing logic for the same topic in different contexts:

```php
'workers' => [
    // Main worker
    'products' => App\Kafka\Consumers\ProductsConsumer::class,

    // Secondary worker — a different handler for the same topic
    'products-analytics' => [
        'topic_key' => 'products',
        'handler'   => App\Kafka\Consumers\ProductsAnalyticsConsumer::class,
    ],
],
```

::: warning
Each such worker must use a **separate** `group.id`, or they'll compete for the same partitions and only one of them will receive any given message.
:::

You can set a separate group right in the worker's config:

```php
'products-analytics' => [
    'topic_key' => 'products',
    'handler'   => App\Kafka\Consumers\ProductsAnalyticsConsumer::class,
    'additional_options' => [
        'group.id' => env('APP_NAME').'-analytics',
    ],
],
```

A worker's `additional_options` are merged with the global ones by key, so only `group.id` is overridden here.

## Inspecting the Configuration

```bash
php artisan kafka:worker:list
```

This command prints every worker, its topics, handlers, and both middleware chains — a convenient way to confirm the config assembled the way you intended.
