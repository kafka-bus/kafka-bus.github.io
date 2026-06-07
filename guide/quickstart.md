# Быстрый старт

В этом разделе показан минимальный рабочий пример — от установки до первого опубликованного и полученного сообщения.

## Выберите способ использования

::: tip Есть Laravel?
Если вы используете Laravel, переходите к [Laravel-интеграции](/laravel/installation) — там автодискавери, конфиг из одного файла и Artisan-команды для управления воркерами.
:::

## Установка

```bash
composer require micromus/kafka-bus
```

**Требования:**

- PHP `^8.2`
- Расширение `ext-rdkafka` и доступный кластер Kafka
- `ext-pcntl` — опционально, для корректной обработки сигналов остановки воркера

## Минимальный пример: publish + consume

Ниже — полный рабочий пример: регистрация топика, воркера, публикация сообщения и запуск слушателя.

```php
<?php

use Micromus\KafkaBus\Bus;
use Micromus\KafkaBus\Connections\Registry\ConnectionRegistry;
use Micromus\KafkaBus\Consumers\Router\ConsumerRoutesBuilder;
use Micromus\KafkaBus\Consumers\Router\RouteInfo;
use Micromus\KafkaBus\Bus\Publishers\Router\PublisherRoutesBuilder;
use Micromus\KafkaBus\Producers\Messages\ProducerMessage;
use Micromus\KafkaBus\Topics\Topic;
use Micromus\KafkaBus\Topics\TopicRegistry;

require __DIR__ . '/vendor/autoload.php';

// 1. Регистрируем топики: 'products' -> реальное имя в Kafka
$topicRegistry = (new TopicRegistry())
    ->add(new Topic('production.fact.products.1', 'products'));

// 2. Определяем обработчик входящих сообщений
class ProductHandler
{
    public function __invoke(string $payload): void
    {
        echo "Получено: {$payload}" . PHP_EOL;
    }
}

// 3. Создаём маршруты для consumer
$consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', new ProductHandler()))
    ->build();

// 4. Создаём маршруты для producer
$publisherRoutes = PublisherRoutesBuilder::make($topicRegistry)
    ->add(ProducerMessage::class, 'products')
    ->build();

// 5. Регистрируем воркер
$workerRegistry = Bus\Listeners\Workers\MemoryWorkerRegistry::make()
    ->add(
        new Bus\Listeners\Workers\Worker(
            name: 'products-worker',
            routes: $consumerRoutes,
            options: new Bus\Listeners\Workers\Options(
                additionalOptions: [
                    'group.id'          => 'my-service',
                    'auto.offset.reset' => 'earliest',
                ]
            )
        )
    );

// 6. Собираем Bus
$bus = new Bus(
    new Bus\ThreadRegistry(
        ConnectionRegistry::default(),
        new Bus\ThreadFactory(
            new Bus\Listeners\ListenerFactory(workerRegistry: $workerRegistry),
            new Bus\Publishers\PublisherFactory(routes: $publisherRoutes),
        )
    ),
    ConnectionRegistry::DEFAULT_CONNECTION_NAME
);

// 7. Публикуем сообщение
$bus->publish(new ProducerMessage(
    payload: json_encode(['id' => 1, 'name' => 'Laptop']),
    headers: ['source' => 'my-service']
));

// 8. Запускаем слушатель (блокирующий цикл)
if (extension_loaded('pcntl')) {
    pcntl_async_signals(true);
    $listener = $bus->listener('products-worker');
    pcntl_signal(SIGINT, fn () => $listener->forceStop());
    $listener->listen();
}
```

## Только публикация

Если нужна только отправка сообщений, воркеры можно не конфигурировать:

```php
$bus->publish(new ProducerMessage(payload: 'hello'));
```

## Только потребление

Если нужно только читать сообщения, достаточно настроить воркер и вызвать `listen()`:

```php
$listener = $bus->listener('products-worker');
$listener->listen(); // блокирующий цикл
```

## Следующие шаги

- [Конфигурация соединений](/core/configuration) — несколько кластеров, SASL, SSL
- [Топики](/core/topics) — реестр топиков и префиксы
- [Producer](/core/producers) — маршруты, опции, flush
- [Consumer](/core/consumers) — типы обработчиков и кастомные фабрики
- [Pipeline](/core/pipeline) — middleware для входящих и исходящих сообщений
- [Тестирование](/core/testing) — фейки без реального брокера
