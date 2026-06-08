# Consumer

## Creating a Handler

Create a class in `app/Kafka/Consumers/`. The argument type of `__invoke` determines how the message will be delivered:

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

### Other Argument Type Options

```php
// Payload as a plain string
public function __invoke(string $payload): void { ... }

// Payload as a decoded JSON array
public function __invoke(array $data): void { ... }

// Original rdkafka object (offset, partition, key)
public function __invoke(\RdKafka\Message $message): void { ... }
```

## Registering in Config

```php
// config/kafka-bus.php
'consumers' => [
    'workers' => [
        'products' => App\Kafka\Consumers\ProductsConsumer::class,
    ],
],
```

## Handler with DomainMessage

When using the `kafka-bus/messages` package, add the `#[MessageFactory]` attribute:

```php
use KafkaBus\Core\Consumers\Attributes\MessageFactory;
use KafkaBus\Messages\Factories\DomainMessageFactory;
use App\Kafka\Messages\ProductMessage;

class ProductsConsumer
{
    #[MessageFactory(new DomainMessageFactory(ProductMessage::class))]
    public function __invoke(ProductMessage $message): void
    {
        match ($message->event->value) {
            'create' => $this->onCreate($message),
            'update' => $this->onUpdate($message),
            'delete' => $this->onDelete($message),
        };
    }

    private function onCreate(ProductMessage $message): void
    {
        Product::create(['id' => $message->id, 'name' => $message->name]);
    }

    private function onUpdate(ProductMessage $message): void
    {
        Product::find($message->id)?->update(
            collect($message->dirty)->mapWithKeys(fn($k) => [$k => $message->$k])->all()
        );
    }

    private function onDelete(ProductMessage $message): void
    {
        Product::find($message->id)?->delete();
    }
}
```

## Starting a Worker

```bash
php artisan kafka:consume products
```

The command starts a blocking read loop. For production, use Supervisor or an equivalent.

### Supervisor Config

```ini
[program:kafka-products]
command=php /var/www/artisan kafka:consume products
directory=/var/www
autostart=true
autorestart=true
stopwaitsecs=60
user=www-data
stdout_logfile=/var/www/storage/logs/kafka-products.log
stderr_logfile=/var/www/storage/logs/kafka-products-err.log
```

## Worker with Multiple Topics

A single worker can listen to multiple topics simultaneously:

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

## Worker-level Middleware

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

## Topic-level Middleware

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

## Multiple Workers for the Same Topic

When you need different processing logic for the same topic in different contexts:

```php
'workers' => [
    // Primary worker
    'products' => App\Kafka\Consumers\ProductsConsumer::class,

    // Secondary worker — different handler for the same topic
    'products-analytics' => [
        'topic_key' => 'products',
        'handler'   => App\Kafka\Consumers\ProductsAnalyticsConsumer::class,
    ],
],
```

::: warning
Each worker must use a **separate** `group.id`, otherwise they will compete for the same partitions.
:::

```dotenv
# For the primary worker
KAFKA_CONSUMER_GROUP_ID=my-service

# For the analytics worker — set in code or a separate .env
```
