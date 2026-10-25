---
title: Маршруты консьюмера
description: Привязка топиков к обработчикам через ReceiverBuilder, middleware маршрута, ленивый ресивер и фабрика сообщений по умолчанию
---

# Маршруты консьюмера

Маршрут консьюмера связывает топик с обработчиком. Маршруты собираются в ресивер, а ресивер передаётся в `Bus`. Когда сообщение передаётся в шину, она ищет маршрут по топику сообщения и запускает его обработчик.

Маршруты не зависят от воркеров: воркер решает, какие топики читать, маршрут — что делать с сообщением из топика. О чтении см. [Воркеры](/ru/docs/2.x/consumer/workers).

## Поток обработки

```
Воркер (kafka-bus/worker)
    │ getMessage()
    ▼
  Bus::dispatch(ConsumerMessageInterface)
    ▼
  Receiver → ReceiverRouter
    │ находит Route по физическому имени топика
    ▼
  RouteExecutor
    │ → MessageFactory (десериализация)
    │ → ReceiverPipelineHandler + middleware маршрута
    ▼
  Обработчик (бизнес-логика)
    │
    ▼ снова в воркере: commit()
```

## Сборка ресивера

`ReceiverBuilder` связывает логические ключи топиков с обработчиками и собирает `Receiver`:

```php
use KafkaBus\Core\Bus;
use KafkaBus\Core\Receivers\Routing\ReceiverBuilder;
use KafkaBus\Core\Receivers\Routing\RouteInfo;

$receiver = ReceiverBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', new ProductHandler()))
    ->add(new RouteInfo('orders', new OrderHandler()))
    ->build();

$bus = new Bus(
    connection: $connection,
    receiver: $receiver,
);
```

Методы `ReceiverBuilder`:

| Метод | Возвращает | Описание |
|---|---|---|
| `make(TopicRegistry $registry, ?MessageFactoryInterface $factory = null)` | `ReceiverBuilder` | Создаёт билдер; `$factory` — фабрика сообщений по умолчанию |
| `add(RouteInfo $routeInfo)` | `self` | Добавляет маршрут; ключ топика резолвится сразу |
| `build()` | `Receiver` | Сразу собирает ресивер |
| `lazy()` | `LazyReceiver` | Откладывает сборку до первого сообщения, см. [Ленивый ресивер](#ленивыи-ресивер) |
| `fake()` | `ReceiverFaker` | Собирает ресивер, который ещё и записывает переданные сообщения, — для тестов |

::: warning Один обработчик на топик
Маршруты индексируются по физическому имени топика. Второй маршрут для того же топика заменяет первый — внутри одной шины у топика ровно один обработчик.
:::

### Параметры маршрута

```php
new RouteInfo(
    topicKey: 'orders',             // логический ключ топика
    handler: new OrderHandler(),    // любой callable
    middleware: [],                 // list<ReceiverPipelineMiddleware>
);
```

| Параметр | Тип | По умолчанию | Описание |
|---|---|---|---|
| `topicKey` | `string` | — | Ключ топика из `TopicRegistry` |
| `handler` | `callable` | — | Обработчик сообщения |
| `middleware` | `list<ReceiverPipelineMiddleware>` | `[]` | Middleware этого маршрута |

### Маршрут с middleware

```php
$receiver = ReceiverBuilder::make($topicRegistry)
    ->add(new RouteInfo(
        topicKey: 'orders',
        handler: new OrderHandler(),
        middleware: [new LoggingMiddleware($logger), new TenantMiddleware()],
    ))
    ->build();
```

На уровне ядра нет «глобальных» middleware консьюмера: общие middleware перечисляйте в каждом маршруте. См. [Pipeline консьюмера](/ru/docs/2.x/consumer/pipeline).

### Фабрика сообщений по умолчанию

Второй аргумент `ReceiverBuilder::make()` задаёт фабрику, которая используется, если тип аргумента обработчика определить не удалось:

```php
use KafkaBus\Core\Receivers\Messages\StringMessageFactory;

$receiver = ReceiverBuilder::make($topicRegistry, new StringMessageFactory())
    ->add(new RouteInfo('products', $legacyHandler))
    ->build();
```

Без второго аргумента используется `NativeMessageFactory`. Приоритет выбора фабрики описан в разделе [Получение сообщений](/ru/docs/2.x/consumer/consuming#порядок-разрешения-фабрики).

## Ленивый ресивер

При сборке ресивера сразу создаются все обработчики и middleware. Если у обработчиков тяжёлые зависимости, а процесс в основном публикует, отложите эту работу через `lazy()`:

```php
$bus = new Bus(
    connection: $connection,
    receiver: ReceiverBuilder::make($topicRegistry)
        ->add(new RouteInfo('products', new ProductHandler()))
        ->lazy(),
);
```

`LazyReceiver` собирает настоящий ресивер при первом `dispatch()` и дальше переиспользует его. Для более тонкого контроля коллекция маршрутов может хранить отдельные объекты `LazyRoute` — см. [Маршруты и ленивое построение](/ru/docs/2.x/architecture#маршруты-и-ленивое-построение).

## Низкоуровневая коллекция маршрутов

`ReceiverBuilder` — тонкая обёртка над `ReceiverRoutesBuilder` и `ReceiverRouter`. Собрать коллекцию самостоятельно можно так:

```php
use KafkaBus\Core\Receivers\Receiver;
use KafkaBus\Core\Receivers\Routing\ReceiverRouter;
use KafkaBus\Core\Receivers\Routing\ReceiverRoutesBuilder;

$routes = ReceiverRoutesBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', new ProductHandler()))
    ->build(); // RouteCollection<Route>

$receiver = new Receiver(new ReceiverRouter($routes));
```

::: warning
`Receivers\Routing\Route`, созданный напрямую через `new Route(...)`, по умолчанию использует `NativeMessageFactory` — определение типа аргумента выполняется только в `ReceiverRoutesBuilder::add()`.
:::
