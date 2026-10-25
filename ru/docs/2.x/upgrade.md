---
title: Переход с 1.x
description: Несовместимые изменения Kafka Bus 2.0 и пошаговое руководство по переводу приложения с 1.x
---

# Переход с 1.x

В Kafka Bus 2.0 переработан приём сообщений. В 1.x `Listener` сам читал Kafka и маршрутизировал сообщения в обход `Bus`. В 2.x `Bus` — единая точка входа в обе стороны: публикация идёт через `Bus::publish()`, приём — через `Bus::dispatch()`, а чтение из Kafka вынесено в отдельный пакет `kafka-bus/worker`.

На этой странице перечислены все несовместимые изменения и для каждого показан код «было → стало». Читайте её сверху вниз, если обновляете приложение на чистом PHP, или сразу переходите к разделу [Laravel Bridge 2.0](#laravel-bridge-2-0), если используете интеграцию с Laravel.

## Коротко об изменениях

| Область | 1.x | 2.x |
|---|---|---|
| Пакеты | `core`, `messages`, `commiter` | плюс `worker` и `metadata`; мета-пакет `kafka-bus/kafka-bus` |
| Подключения | `Bus` держит несколько подключений, `onConnection()` | один `Bus` = одно подключение |
| Маршруты консьюмера | хранятся в каждом `Worker` | хранятся в `Bus` (`Receiver`), один обработчик на топик |
| Чтение Kafka | `$bus->listener('name')->listen()` | `WorkerRunnerFactory::create($bus, 'name')->run()` |
| Middleware консьюмера | два уровня: воркер и маршрут | один уровень: маршрут (`ReceiverPipelineMiddleware`) |
| Оффсеты | `Listener::partitions()` | `kafka-bus/metadata`: `Metadata::fromConnection()` |
| Тестирование | `ConnectionRegistryFaker` + ручные проверки | `BusFaker` со встроенными проверками |
| Пространства имён | `KafkaBus\Core\Interfaces\…`, `Bus\…` | контракты лежат рядом с реализациями |

## Зависимости

Все пять пакетов выпускаются вместе под одним тегом. Проще всего поставить мета-пакет — он заменяет `core`, `messages`, `commiter`, `worker` и `metadata`:

```bash
composer require kafka-bus/kafka-bus:^2.1.1
```

Если вы ставите пакеты по отдельности, поднимите их все до `^2.0` и добавьте нужные новые:

```json
{
    "require": {
        "kafka-bus/core": "^2.0",
        "kafka-bus/messages": "^2.0",
        "kafka-bus/commiter": "^2.0",
        "kafka-bus/worker": "^2.0",
        "kafka-bus/metadata": "^2.0"
    }
}
```

`kafka-bus/worker` нужен для чтения из Kafka, `kafka-bus/metadata` — для просмотра и сброса оффсетов. Сервису, который только публикует, достаточно `kafka-bus/core`.

## Пространства имён

Дерева `KafkaBus\Core\Interfaces\…` больше нет — каждый контракт лежит рядом со своей реализацией. Поддерево `Bus\…` разделено на `Publishers\…` и `Receivers\…`.

::: details Полная карта классов kafka-bus/core
| 1.x | 2.x |
|---|---|
| `Interfaces\Bus\BusInterface` | `BusInterface` |
| `Interfaces\Connections\ConnectionInterface` | `Connections\ConnectionInterface` |
| `Interfaces\Connections\ConnectionConfigInterface` | `Connections\Config\ConnectionConfigInterface` |
| `Interfaces\Connections\ConnectionRegistryInterface` | `Connections\Registry\ConnectionRegistryInterface` |
| `Interfaces\Connections\SaslConfigurationConfigInterface` | `Connections\Config\SaslConfigurationConfigInterface` |
| `Interfaces\Consumers\ConsumerInterface` | `Consumers\ConsumerInterface` |
| `Interfaces\Consumers\Messages\ConsumerMessageInterface` | `Consumers\Messages\ConsumerMessageInterface` |
| `Interfaces\Consumers\Messages\MessageFactoryInterface` | `Receivers\Messages\MessageFactoryInterface` |
| `Interfaces\Producers\ProducerInterface` | `Producers\ProducerInterface` |
| `Interfaces\Producers\Messages\ProducerMessageInterface` | `Producers\Messages\ProducerMessageInterface` |
| `Interfaces\Producers\Messages\HasKey` | `Producers\Messages\HasKey` |
| `Interfaces\Producers\Messages\HasHeaders` | `Producers\Messages\HasHeaders` |
| `Interfaces\Producers\Messages\HasPartition` | `Producers\Messages\HasPartition` |
| `Interfaces\Pipelines\PipelineInterface` | `Pipelines\PipelineInterface` |
| `Interfaces\Pipelines\PipelineHandlerInterface` | `Pipelines\PipelineHandlerInterface` |
| `Interfaces\Pipelines\PipelineMiddlewareInterface` | `Pipelines\PipelineMiddlewareInterface` |
| `Bus\MessageBatch` | `Publishers\MessageBatch` |
| `Bus\Publishers\Publisher` | `Publishers\Publisher` |
| `Bus\Publishers\PublisherFactory` | `Publishers\PublisherFactory` |
| `Bus\Publishers\Router\PublisherRoutesBuilder` | `Publishers\Routing\PublisherRoutesBuilder` |
| `Bus\Publishers\Router\Route` | `Publishers\Routing\Route` |
| `Bus\Publishers\Router\Options` | `Publishers\Routing\Options` |
| `Bus\Publishers\Router\PublisherRoutes` | `Routing\RouteCollection` |
| `Producers\ProducerStream` | `Publishers\PublisherStream` |
| `Producers\ProducerStreamFactory` | `Publishers\PublisherStreamFactory` |
| `Producers\Pipelines\ProducerPipelineHandler` | `Publishers\Pipelines\PublisherPipelineHandler` |
| `Producers\Pipelines\ProducerPipelineMiddleware` | `Publishers\Pipelines\PublisherPipelineMiddleware` |
| `Consumers\Router\ConsumerRoutesBuilder` | `Receivers\Routing\ReceiverRoutesBuilder` (или `ReceiverBuilder`) |
| `Consumers\Router\ConsumerRoutes` | `Routing\RouteCollection` |
| `Consumers\Router\Route` | `Receivers\Routing\Route` |
| `Consumers\Router\RouteInfo` | `Receivers\Routing\RouteInfo` |
| `Consumers\Router\RouteExecutor` | `Receivers\Routing\RouteExecutor` |
| `Consumers\Attributes\MessageFactory` | `Receivers\Attributes\MessageFactory` |
| `Consumers\Messages\NativeMessageFactory` | `Receivers\Messages\NativeMessageFactory` |
| `Consumers\Messages\StringMessageFactory` | `Receivers\Messages\StringMessageFactory` |
| `Consumers\Messages\JsonMessageFactory` | `Receivers\Messages\JsonMessageFactory` |
| `Consumers\Messages\OriginalMessageFactory` | `Receivers\Messages\OriginalMessageFactory` |
| `Consumers\Pipelines\ConsumerPipelineHandler` | `Receivers\Pipelines\ReceiverPipelineHandler` |
| `Consumers\Pipelines\ConsumerPipelineMiddleware` | `Receivers\Pipelines\ReceiverPipelineMiddleware` |
| `Consumers\Pipelines\MessagePipelineMiddleware` | `Receivers\Pipelines\ReceiverPipelineMiddleware` |
| `Support\RetryRepeater` | `Utils\RetryRepeater` |
:::

::: details Классы, перенесённые в kafka-bus/worker
| 1.x (`KafkaBus\Core\…`) | 2.x (`KafkaBus\Worker\…`) |
|---|---|
| `Bus\Listeners\Workers\Worker` | `Worker` |
| `Bus\Listeners\Workers\Options` | `Options` |
| `Bus\Listeners\Workers\MemoryWorkerRegistry` | `Registry\MemoryWorkerRegistry` |
| `Interfaces\Bus\Listeners\WorkerRegistryInterface` | `Registry\WorkerRegistryInterface` |
| `Bus\Listeners\Workers\WorkerMerger` | `WorkerMerger` |
| `Consumers\ConsumerStream` | `Consumers\ConsumerStream` |
| `Interfaces\Consumers\ConsumerStreamInterface` | `Consumers\ConsumerStreamInterface` |
| `Exceptions\Listeners\ListenerException` | `Exceptions\WorkerException` |
:::

::: details Классы, перенесённые в kafka-bus/metadata
| 1.x (`KafkaBus\Core\…`) | 2.x (`KafkaBus\Metadata\…`) |
|---|---|
| `Bus\Listeners\Partitions\Partitions` | `Partitions\Partitions` |
| `Interfaces\Bus\Listeners\PartitionsInterface` | `Partitions\PartitionsInterface` |
| `Bus\Listeners\Partitions\CommitOffset` | `Partitions\CommitOffset` |
| `Bus\Listeners\Partitions\CommitOffsetResult` | `Partitions\CommitOffsetResult` |
| `Bus\Listeners\Partitions\Offset` | `Partitions\Offset` |
| `Bus\Listeners\Partitions\TopicPartition` | `Partitions\TopicPartition` |
| `Connections\Topics\Topics` | `Topics\TopicsMetadata` |
| `Exceptions\Listeners\CannotCommitOffsetException` | `Exceptions\CannotCommitOffsetException` |
| `Exceptions\TopicNotFoundException` | `Exceptions\TopicNotFoundException` |
:::

Удалены без замены: `Thread`, `ThreadFactory`, `ThreadRegistry`, `ThreadInterface`, `Listener`, `ListenerFactory`, `ListenerInterface`, `MessageHandler`, `MessageHandlerFactory`, `RouteHandler`, `WorkerConsumerMessage`, `ConsumerMeta`, `ConnectionHasTopicsInterface`, `ConnectionRegistryFaker`.

У `kafka-bus/messages` собственные пространства имён не изменились — поменялись только импорты классов ядра.

## Сборка шины

`Bus` больше не принимает `ThreadRegistry`. Шина привязана к одному подключению и получает маршруты продюсера и ресивер напрямую:

```php
use KafkaBus\Core\Bus;

$bus = new Bus( // [!code --]
    new Bus\ThreadRegistry( // [!code --]
        $connectionRegistry, // [!code --]
        new Bus\ThreadFactory( // [!code --]
            new Bus\Listeners\ListenerFactory(workerRegistry: $workerRegistry), // [!code --]
            new Bus\Publishers\PublisherFactory(routes: $publisherRoutes), // [!code --]
        ), // [!code --]
    ), // [!code --]
    defaultConnection: 'kafka', // [!code --]
); // [!code --]
$bus = new Bus( // [!code ++]
    connection: $connectionRegistry->connection('kafka'), // [!code ++]
    publisherFactory: new PublisherFactory(routes: $publisherRoutes), // [!code ++]
    receiver: $receiver, // [!code ++]
); // [!code ++]
```

`ConnectionRegistry`, `KafkaConnectionConfig`, `UserCredentialsConfig` и `DriverRegistry` работают как раньше. См. [Конфигурация](/ru/docs/2.x/configuration#сборка-шины).

### Несколько подключений

`Bus::onConnection()` удалён. Чтобы публиковать во второй кластер, соберите вторую шину на этом подключении:

```php
$bus->onConnection('analytics')->publish($message); // [!code --]
$analyticsBus = new Bus($connectionRegistry->connection('analytics'), $publisherFactory); // [!code ++]
$analyticsBus->publish($message); // [!code ++]
```

## Публикация

Публичный API (`publish()`, `publishBatch()`, `MessageBatch`) не изменился, кроме пространств имён. Переехали две вещи:

- `Bus::routes()` удалён — используйте `$bus->publisher()->routes()` (метод есть у класса `Bus`, но не у `BusInterface`);
- `PublisherRoutesBuilder::build()` возвращает `RouteCollection` вместо `PublisherRoutes`.

Middleware продюсера должны реализовывать переименованный интерфейс:

```php
use KafkaBus\Core\Producers\Pipelines\ProducerPipelineHandler; // [!code --]
use KafkaBus\Core\Producers\Pipelines\ProducerPipelineMiddleware; // [!code --]
use KafkaBus\Core\Publishers\Pipelines\PublisherPipelineHandler; // [!code ++]
use KafkaBus\Core\Publishers\Pipelines\PublisherPipelineMiddleware; // [!code ++]

final class TracingMiddleware implements ProducerPipelineMiddleware // [!code --]
final class TracingMiddleware implements PublisherPipelineMiddleware // [!code ++]
```

Методы обработчика (`target()`, `topic()`, `withHeader()`) остались прежними.

## Приём сообщений

### Маршруты живут в шине

В 1.x у каждого `Worker` была своя таблица маршрутов. В 2.x маршруты принадлежат `Receiver` внутри `Bus`, а `Worker` только перечисляет топики для чтения:

```php
// 1.x
$consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', new ProductHandler()))
    ->build();

$workerRegistry = MemoryWorkerRegistry::make()
    ->add(new Worker('products', $consumerRoutes, new Options(/* ... */)));
```

```php
// 2.x
use KafkaBus\Core\Receivers\Routing\ReceiverBuilder;
use KafkaBus\Core\Receivers\Routing\RouteInfo;
use KafkaBus\Worker\Options;
use KafkaBus\Worker\Registry\MemoryWorkerRegistry;
use KafkaBus\Worker\Worker;

$receiver = ReceiverBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', new ProductHandler()))
    ->build();

$workerRegistry = MemoryWorkerRegistry::make()
    ->add(new Worker('products', [$topicRegistry->get('products')], new Options(/* ... */)));
```

::: warning Один обработчик на топик
Внутри одной шины маршруты индексируются по физическому имени топика. В 1.x два воркера могли привязать к одному топику разные обработчики. В 2.x второй `add()` для того же топика заменяет первый. Чтобы обрабатывать один топик двумя разными способами, запустите два приложения (или две шины) с разными `group.id` либо вызывайте оба обработчика из одного.
:::

### Запуск воркера

`Bus::listener()` удалён. Цикл чтения создаёт `WorkerRunnerFactory` из `kafka-bus/worker`:

```php
$listener = $bus->listener('products'); // [!code --]
$listener->listen(); // [!code --]
$runner = (new WorkerRunnerFactory($workerRegistry))->create($bus, 'products'); // [!code ++]
$runner->run(); // [!code ++]
```

`forceStop()` переехал из `Listener` в `WorkerRunner`. Объединение нескольких воркеров в один процесс (`create($bus, ['products', 'orders'])`) по-прежнему работает — теперь объединяются только списки топиков. См. [Воркеры](/ru/docs/2.x/consumer/workers).

### Middleware

Два уровня middleware консьюмера объединены в один. И `ConsumerPipelineMiddleware` (уровень воркера), и `MessagePipelineMiddleware` (уровень маршрута) становятся `ReceiverPipelineMiddleware` и подключаются к маршруту:

| Что было в 1.x | Что использовать в 2.x |
|---|---|
| `Workers\Options::$middleware` | `RouteInfo::$middleware` в каждом маршруте, где он нужен |
| `ConsumerPipelineHandler::target()` — сырое сообщение | `ReceiverPipelineHandler::target()` — сырое сообщение |
| `MessagePipelineHandler::target()` — десериализованный объект | `ReceiverPipelineHandler::formatted()` — десериализованный объект |

```php
final class CheckingMiddleware implements MessagePipelineMiddleware // [!code --]
final class CheckingMiddleware implements ReceiverPipelineMiddleware // [!code ++]
{
    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        $order = $pipeline->handler()->target(); // [!code --]
        $order = $pipeline->handler()->formatted(); // [!code ++]

        // ...
    }
}
```

Middleware уровня воркера — логирование, `ConsumerCommiterMiddleware` и т. п. — теперь нужно указать в каждом маршруте. См. [Pipeline консьюмера](/ru/docs/2.x/consumer/pipeline).

::: tip
Исключения обработчика теперь доходят до middleware маршрута **без обёртки**. `Receiver::dispatch()` оборачивает их в `MessageConsumerNotHandledException` только после завершения pipeline.
:::

## Режим коммита оффсетов

В `kafka-bus/core` 1.3.1 и 2.1.1 изменился смысл `autoCommit` — раньше коммитеры были перепутаны:

| `autoCommit` | 1.3.0 и ранее, 2.0.0–2.1.0 | 1.3.1+, 2.1.1+ |
|---|---|---|
| `true` | явный `commitAsync()` после каждого сообщения (плюс фоновые коммиты librdkafka) | только фоновые коммиты librdkafka |
| `false` | ничего не коммитится | явный `commitAsync()` после каждого сообщения |

Если вы рассчитывали на «коммит после каждого обработанного сообщения» с `autoCommit: true`, после обновления установите `autoCommit: false`. В Laravel `auto_commit` по умолчанию `false`, поэтому по умолчанию теперь коммит идёт после каждого сообщения. См. [Коммит оффсетов](/ru/docs/2.x/consumer/workers#коммит-оффсетов).

## Оффсеты

`Listener::partitions()` заменён пакетом `kafka-bus/metadata`. Он не зависит от воркеров — топики и группа консьюмеров передаются явно:

```php
$partitions = $bus->listener('products')->partitions(); // [!code --]
$partitions = Metadata::fromConnection($connection) // [!code ++]
    ->partitions([$topicRegistry->get('products')], new ConsumerConfig( // [!code ++]
        additionalOptions: ['group.id' => 'products-service'], // [!code ++]
    )); // [!code ++]

$partitions->list();
$partitions->setOffset(new CommitOffset($topic, RD_KAFKA_PARTITION_UA, Offset::Early));
```

`list()` и `setOffset()` работают как раньше. См. [Metadata](/ru/docs/2.x/components/metadata).

## Commiter

| 1.x | 2.x |
|---|---|
| `ProducerIdempotencyMiddleware` | `PublisherIdempotencyMiddleware` |
| `ConsumerCommiterMiddleware` на воркере | `ConsumerCommiterMiddleware` на маршруте |

`ConsumerCommiterMiddleware` теперь реализует `ReceiverPipelineMiddleware`; конструктор и поведение не изменились. Репозитории и источники хранения не изменились.

## Тестирование

`ConnectionRegistryFaker` удалён, `ConnectionFaker` больше не принимает `TopicRegistry`. Вместо них соберите `BusFaker` — он подключает фейковое соединение, записывающий ресивер и содержит готовые проверки:

```php
use KafkaBus\Core\Testing\Assertions\PHPUnitAssertionDriver;
use KafkaBus\Core\Testing\BusFaker;

$bus = BusFaker::make(
    $topicRegistry,
    new PHPUnitAssertionDriver(),
    new PublisherFactory(routes: $publisherRoutes),
    $receiver,
);

$bus->publish(new ProductMessage(/* ... */));

$bus->assertPublished(ProductMessage::class);
```

У `MessageFactory::withTopicKey()` появился необязательный второй аргумент — `TopicRegistry`, чтобы превратить ключ в физическое имя. См. [Тестирование продюсера](/ru/docs/2.x/producer/testing) и [Тестирование консьюмера](/ru/docs/2.x/consumer/testing).

## Исключения

| 1.x | 2.x |
|---|---|
| `ListenerException` — воркер не найден | `KafkaBus\Worker\Exceptions\WorkerException` |
| `Core\Exceptions\Listeners\CannotCommitOffsetException` | `KafkaBus\Metadata\Exceptions\CannotCommitOffsetException` |
| `Core\Exceptions\TopicNotFoundException` | `KafkaBus\Metadata\Exceptions\TopicNotFoundException` |

Остальная иерархия не изменилась.

## Laravel Bridge 2.0

`kafka-bus/laravel-bridge:^2.0` зависит от мета-пакета `kafka-bus/kafka-bus:^2.1` вместо отдельных `core`, `commiter` и `messages`:

```bash
composer require kafka-bus/laravel-bridge:^2.0
```

### Конфигурация

Обработчики переехали из воркеров в новый раздел `consumers.routes` с ключом по топику. Воркеры теперь перечисляют только ключи топиков и параметры консьюмера:

```php
// 1.x — config/kafka-bus.php
'consumers' => [
    'workers' => [
        'products' => App\Kafka\Consumers\ProductsConsumer::class,
        'default' => [
            'middleware' => [App\Kafka\Middleware\LoggingMiddleware::class],
            'topics' => [
                'orders' => [
                    'handler' => App\Kafka\Consumers\OrdersConsumer::class,
                    'middleware' => [App\Kafka\Middleware\TenantMiddleware::class],
                ],
            ],
        ],
    ],
],
```

```php
// 2.x — config/kafka-bus.php
'consumers' => [
    'routes' => [
        'products' => App\Kafka\Consumers\ProductsConsumer::class,
        'orders' => [
            'handler' => App\Kafka\Consumers\OrdersConsumer::class,
            'middleware' => [
                App\Kafka\Middleware\LoggingMiddleware::class,
                App\Kafka\Middleware\TenantMiddleware::class,
            ],
        ],
    ],

    'workers' => [
        'default' => [
            'topics' => ['orders'],
        ],
    ],
],
```

Что изменилось в разделе `consumers`:

- новый раздел `routes`: `ключ топика => класс обработчика` или `['handler' => …, 'middleware' => […]]`;
- `topics` воркера — это список ключей топиков, а не карта `топик => обработчик`;
- ключи воркера `handler`, `topic_key` и `middleware` больше не читаются;
- каждый маршрут одновременно является воркером с тем же именем — `kafka:consume products` работает без записи в `workers`;
- закомментированная подсказка для продюсеров теперь ссылается на `PublisherIdempotencyMiddleware`.

### Artisan-команды

| 1.x | 2.x |
|---|---|
| `kafka:consume:all {--expect=*}` | `kafka:consume:routes {--expect=*}` — все маршруты в одном процессе |
| `kafka:route:list` | `kafka:publisher:route:list` |
| — | `kafka:consumer:route:list` |
| `kafka:offset:show {worker}` | `kafka:consumer:offset:show {topicKey?}` |
| `kafka:offset:set {worker} {topic} {offset}` | `kafka:consumer:offset:set {topicKey} {offset}` |

Команды оффсетов больше не принимают воркер — они используют группу консьюмеров из глобальных `consumers.additional_options`. См. [Команды](/ru/docs/2.x/laravel/commands).

### Фасад и тестирование

- `KafkaBus::onConnection()`, `KafkaBus::routes()` и `KafkaBus::listener()` удалены;
- добавлены `KafkaBus::connection()` и `KafkaBus::dispatch()`;
- у фейка появились `assertDispatched()`, `assertDispatchedTimes()`, `assertNotDispatched()`, `assertNothingDispatched()`, `getDispatched()` и `allDispatched()`;
- `KafkaBus::listen()` принимает имя воркера или список имён.

См. [Тестирование в Laravel](/ru/docs/2.x/laravel/testing).

### Чек-лист обновления

1. Поднимите `kafka-bus/laravel-bridge` до `^2.0`, убедитесь, что `kafka-bus/kafka-bus` разрешается в `2.1.1` или новее, и уберите явные зависимости `kafka-bus/core`, `kafka-bus/commiter`, `kafka-bus/messages`.
2. Перенесите все обработчики из `consumers.workers` в `consumers.routes`.
3. Перенесите middleware воркеров в `middleware` каждого маршрута (или в глобальный `consumers.middleware`).
4. Замените `ProducerIdempotencyMiddleware` на `PublisherIdempotencyMiddleware`.
5. Обновите импорты классов ядра в обработчиках, сообщениях и middleware (см. карту классов выше).
6. Сверьте `auto_commit` с новым [режимом коммита](#режим-коммита-оффсетов).
7. Переименуйте Artisan-команды в конфигах Supervisor, расписании и скриптах.
8. Запустите `php artisan kafka:consumer:route:list` и `php artisan kafka:worker:list`, чтобы проверить результат.
