---
title: Получение сообщений в Laravel
description: Обработчики, регистрация маршрутов консьюмера, запуск воркеров через Artisan и их развёртывание под Supervisor
---

# Получение сообщений

В Laravel-приложении получение сообщений состоит из трёх шагов: напишите класс-обработчик, привяжите его к топику в `consumers.routes` и запустите воркер командой `php artisan kafka:consume`. На этой странице разобраны все шаги и типовые конфигурации.

## Создание обработчика

Создайте класс в `app/Kafka/Consumers/`. Тип аргумента `__invoke` определяет, в каком виде придёт сообщение:

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

Обработчик резолвится через контейнер Laravel, поэтому зависимости можно внедрять через конструктор:

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

### Другие типы аргумента

```php
// Payload как строка
public function __invoke(string $payload): void

// Payload как декодированный JSON-массив
public function __invoke(array $data): void

// Оригинальный объект rdkafka (offset, partition, key)
public function __invoke(\RdKafka\Message $message): void
```

Подробнее об определении типа — в разделе [Получение сообщений](/ru/docs/2.x/consumer/consuming#порядок-разрешения-фабрики).

## Регистрация маршрута

Привяжите обработчик к ключу топика в `consumers.routes`:

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

У каждого топика один обработчик. Форматы и параметры маршрутов — в разделе [Конфигурация](/ru/docs/2.x/laravel/configuration#маршруты).

## Типизированный обработчик

С `kafka-bus/messages` добавьте атрибут `#[MessageFactory]`:

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
Событие и список изменённых полей доступны через методы `getEvent()` и `getDirty()`, а не как свойства. `$message->event` вернёт `null`, потому что обращение уйдёт в магический геттер `Payload`.
:::

## Запуск воркера

Каждый маршрут одновременно является воркером с тем же именем, поэтому один топик можно читать сразу:

```bash
php artisan kafka:consume products
```

Команда запускает блокирующий цикл чтения и обрабатывает `SIGTERM`, `SIGINT` и `SIGQUIT`: текущее сообщение дообрабатывается и коммитится, затем процесс завершается.

### Несколько топиков в одном процессе

Объявите воркер со списком топиков:

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

Можно передать и несколько имён воркеров — их топики объединятся в один консьюмер:

```bash
php artisan kafka:consume products orders
```

При объединении параметры консьюмера (`group.id`, `auto_commit`, `consumer_timeout`) берутся у первого воркера в списке. Объединяйте только воркеры с совместимыми настройками — см. [Воркеры](/ru/docs/2.x/consumer/workers#несколько-воркеров-в-одном-процессе).

Все топики одного процесса читает один консьюмер в одной группе — сообщения обрабатываются последовательно, порядок между топиками не гарантируется.

### Все маршруты сразу

```bash
php artisan kafka:consume:routes
php artisan kafka:consume:routes --expect=orders
```

Команда читает все топики из `consumers.routes` в одном процессе с глобальными параметрами консьюмера; `--expect` исключает ключи топиков. Удобно для локальной разработки и небольших сервисов.

## Параметры воркера

Воркер может переопределить глобальные настройки консьюмера:

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

`additional_options` воркера сливаются с глобальными по ключу, поэтому перекрываются только перечисленные ключи.

## Middleware

Глобальные middleware из `consumers.middleware` добавляются к каждому маршруту. Middleware конкретного маршрута указываются в самом маршруте:

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

Middleware указываются классами и резолвятся через контейнер. Они реализуют `ReceiverPipelineMiddleware` и видят и сырое сообщение (`target()`), и десериализованное (`formatted()`). См. [Pipeline консьюмера](/ru/docs/2.x/consumer/pipeline).

::: info Изменено в 2.0
У воркеров больше нет собственных `middleware`. Middleware принадлежат маршрутам, поэтому применяются к топику независимо от того, какой воркер его читает.
:::

## Один топик в нескольких группах

У топика один обработчик на приложение. Чтобы читать один топик двумя независимыми способами, запустите два воркера с разными `group.id` — оба передадут сообщения в один и тот же обработчик:

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

Если логика обработки должна различаться, разветвите её внутри обработчика или вынесите второй консьюмер в отдельное приложение.

::: warning
Воркеры, читающие один топик с одинаковым `group.id`, делят его партиции — каждое сообщение получит только один из них.
:::

## Конфигурация Supervisor

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
`stopwaitsecs` должен быть больше `consumer_timeout`, иначе Supervisor убьёт процесс, не дав завершить текущую итерацию.

Увеличивать `numprocs` имеет смысл только до числа партиций топика: лишние процессы в той же группе не получат партиций.
:::

::: warning Если обработчик падает, падает и процесс
Неперехваченное исключение из обработчика останавливает воркер, и Supervisor его перезапускает. Без защиты «ядовитое» сообщение вызывает бесконечный цикл перезапусков.

Добавьте middleware с `try/catch` ([пример](/ru/docs/2.x/consumer/pipeline#middleware-с-обработкои-ошибок)) или `ConsumerCommiterMiddleware` с лимитом `maxAttempt`.
:::

## Проверка конфигурации

```bash
php artisan kafka:consumer:route:list
php artisan kafka:worker:list
```

Первая команда выводит каждый топик с обработчиком и итоговой цепочкой middleware, вторая — каждый воркер и его топики. См. [Команды](/ru/docs/2.x/laravel/commands).
