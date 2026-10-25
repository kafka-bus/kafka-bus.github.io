# Получение сообщений

## Создание обработчика

Создайте класс в `app/Kafka/Consumers/`. Тип аргумента `__invoke` определяет, как будет доставлено сообщение:

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

        // Бизнес-логика
        Product::updateOrCreate(
            ['id' => $payload['id']],
            ['name' => $payload['name'], 'status' => $payload['status']],
        );
    }
}
```

Обработчик резолвится через контейнер Laravel, поэтому зависимости можно внедрять через конструктор:

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

### Другие варианты типов аргументов

```php
// Payload как обычная строка
public function __invoke(string $payload): void { ... }

// Payload как декодированный JSON массив
public function __invoke(array $data): void { ... }

// Оригинальный объект rdkafka (offset, partition, key)
public function __invoke(\RdKafka\Message $message): void { ... }
```

Подробнее о механизме автоопределения — в разделе [Получение сообщений](/ru/docs/1.x/consumer/consuming#порядок-разрешения-фабрики).

## Регистрация в конфигурации

```php
// config/kafka-bus.php
'consumers' => [
    'workers' => [
        'products' => App\Kafka\Consumers\ProductsConsumer::class,
    ],
],
```

Имя воркера здесь совпадает с ключом топика. Другие форматы — в разделе [Конфигурация](/ru/docs/1.x/laravel/configuration#консьюмеры).

## Типизированный обработчик

При использовании `kafka-bus/messages` добавьте атрибут `#[MessageFactory]`:

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
Событие и список изменённых полей доступны через методы `getEvent()` и `getDirty()`, а не как свойства. `$message->event` вернёт `null`, потому что пойдёт в магический геттер `Payload`.
:::

## Запуск воркера

```bash
php artisan kafka:consume products
```

Команда запускает блокирующий цикл чтения и корректно обрабатывает `SIGTERM`, `SIGINT` и `SIGQUIT`: текущее сообщение дообрабатывается и коммитится, после чего процесс завершается.

### Несколько воркеров в одном процессе

Команда принимает список имён:

```bash
php artisan kafka:consume products orders
```

Воркеры объединяются, но настройки подключения (`group.id`, `auto_commit`, `consumer_timeout`) берутся у **первого** воркера в списке. Объединяйте только воркеров с совместимыми настройками — подробнее в разделе [Архитектура](/ru/docs/1.x/architecture#несколько-воркеров-в-одном-процессе).

### Все воркеры сразу

```bash
php artisan kafka:consume:all
php artisan kafka:consume:all --expect=products-analytics
```

### Конфигурация Supervisor

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
`stopwaitsecs` должен превышать `consumer_timeout`, иначе Supervisor убьёт процесс до того, как тот завершит текущую итерацию.

Увеличивать `numprocs` имеет смысл только до количества партиций топика: лишние процессы в той же группе останутся без назначенных партиций.
:::

::: warning Обработчик падает — процесс тоже
Необработанное исключение из обработчика останавливает воркер, и Supervisor перезапускает его. Без защиты «ядовитое» сообщение зациклит перезапуски.

Добавьте middleware с `try/catch` ([пример](/ru/docs/1.x/consumer/pipeline#middleware-с-обработкои-ошибок)) или `ConsumerCommiterMiddleware` с ограничением `maxAttempt`.
:::

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

Все топики читаются одним консьюмером в одной группе — сообщения обрабатываются последовательно, порядок между топиками не гарантирован.

## Middleware на уровне воркера

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

Middleware указываются классами и резолвятся через контейнер, поэтому в них можно внедрять зависимости. К этому списку добавляются глобальные из `consumers.middleware` — они выполняются первыми.

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

Middleware топика реализует `MessagePipelineMiddleware` и видит уже десериализованный объект, а middleware воркера — `ConsumerPipelineMiddleware` и видит сырое сообщение. См. [Pipeline](/ru/docs/1.x/consumer/pipeline).

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
Каждый такой воркер должен использовать **отдельный** `group.id`, иначе они будут конкурировать за одни и те же партиции и каждое сообщение достанется только одному из них.
:::

Задать отдельную группу можно прямо в конфигурации воркера:

```php
'products-analytics' => [
    'topic_key' => 'products',
    'handler'   => App\Kafka\Consumers\ProductsAnalyticsConsumer::class,
    'additional_options' => [
        'group.id' => env('APP_NAME').'-analytics',
    ],
],
```

`additional_options` воркера объединяются с глобальными по ключам, поэтому переопределяется только `group.id`.

## Проверка конфигурации

```bash
php artisan kafka:worker:list
```

Команда выводит воркеры, их топики, обработчики и обе цепочки middleware — удобный способ убедиться, что конфиг собрался как задумано.
