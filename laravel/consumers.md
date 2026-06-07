# Consumer (Laravel)

## Создание обработчика

Создайте класс в `app/Kafka/Consumers/`. Тип аргумента `__invoke` определяет, в каком виде придёт сообщение:

```php
// app/Kafka/Consumers/ProductsConsumer.php

namespace App\Kafka\Consumers;

use Micromus\KafkaBus\Interfaces\Consumers\Messages\ConsumerMessageInterface;

class ProductsConsumer
{
    public function __invoke(ConsumerMessageInterface $message): void
    {
        $payload = json_decode($message->payload(), true);
        $headers = $message->headers();

        // Бизнес-логика
        Product::updateOrCreate(
            ['id' => $payload['id']],
            ['name' => $payload['name'], 'status' => $payload['status']],
        );
    }
}
```

### Другие варианты типа аргумента

```php
// Только payload как строка
public function __invoke(string $payload): void { ... }

// Payload как декодированный JSON-массив
public function __invoke(array $data): void { ... }

// Оригинальный объект rdkafka (offset, partition, key)
public function __invoke(\RdKafka\Message $message): void { ... }
```

## Регистрация в конфиге

```php
// config/kafka-bus.php
'consumers' => [
    'workers' => [
        'products' => App\Kafka\Consumers\ProductsConsumer::class,
    ],
],
```

## Обработчик с DomainMessage

При использовании пакета `kafka-bus-messages` добавьте атрибут `#[MessageFactory]`:

```php
use Micromus\KafkaBus\Consumers\Attributes\MessageFactory;
use Micromus\KafkaBusMessages\Factories\DomainMessageFactory;
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

## Запуск воркера

```bash
php artisan kafka:consume products
```

Команда запускает блокирующий цикл чтения. Для продакшна используйте Supervisor или аналог.

### Supervisor-конфиг

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

## Воркер с несколькими топиками

Один воркер может слушать несколько топиков одновременно:

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

## Middleware на уровне воркера

```php
'workers' => [
    'products' => [
        'middleware' => [
            App\Kafka\Middleware\LoggingMiddleware::class,
            \Micromus\KafkaBusCommiter\Middleware\ConsumerCommiterMiddleware::class,
        ],
        'handler' => App\Kafka\Consumers\ProductsConsumer::class,
    ],
],
```

## Middleware на уровне топика

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

## Несколько воркеров для одного топика

Когда нужна разная логика обработки одного топика в разных контекстах:

```php
'workers' => [
    // Основной воркер
    'products' => App\Kafka\Consumers\ProductsConsumer::class,

    // Вторичный воркер — другой обработчик для того же топика
    'products-analytics' => [
        'topic_key' => 'products',
        'handler'   => App\Kafka\Consumers\ProductsAnalyticsConsumer::class,
    ],
],
```

::: warning
Каждый воркер должен использовать **отдельный** `group.id`, иначе они будут конкурировать за одни и те же партиции.
:::

```dotenv
# Для основного воркера
KAFKA_CONSUMER_GROUP_ID=my-service

# Для аналитического — задайте в коде или отдельном .env
```
