---
title: Тестирование продюсера
description: Проверка публикации без брокера Kafka с помощью BusFaker и его встроенных проверок
---

# Тестирование

В `kafka-bus/core` есть `BusFaker` — реализация `BusInterface`, которая записывает всё опубликованное вместо отправки в Kafka и предоставляет готовые проверки. Маршруты, топики и middleware остаются настоящими — подменяется только транспорт.

::: tip Laravel
В Laravel-приложении ничего собирать вручную не нужно — вызовите `KafkaBus::fake()` и используйте те же проверки через фасад. См. [Тестирование в Laravel](/ru/docs/2.x/laravel/testing).
:::

## Создание фейковой шины

`BusFaker::make()` принимает реестр топиков, драйвер проверок и — по желанию — ту же `PublisherFactory`, что и в продакшене:

```php
use KafkaBus\Core\Publishers\PublisherFactory;
use KafkaBus\Core\Publishers\Routing\PublisherRoutesBuilder;
use KafkaBus\Core\Testing\Assertions\PHPUnitAssertionDriver;
use KafkaBus\Core\Testing\BusFaker;
use KafkaBus\Core\Topics\Topic;
use KafkaBus\Core\Topics\TopicRegistry;

$topicRegistry = (new TopicRegistry())
    ->add(new Topic('production.fact.products.1', 'products'));

$routes = PublisherRoutesBuilder::make($topicRegistry)
    ->add(ProductCreatedMessage::class, 'products')
    ->build();

$bus = BusFaker::make(
    $topicRegistry,
    new PHPUnitAssertionDriver(),
    new PublisherFactory(routes: $routes),
);
```

| Параметр | Тип | По умолчанию | Описание |
|---|---|---|---|
| `topicRegistry` | `TopicRegistry` | — | Резолвит ключи топиков в проверках |
| `driver` | `AssertionDriverInterface` | — | Как сообщается о проваленной проверке |
| `publisherFactory` | `PublisherFactory` | `new PublisherFactory()` | Маршруты продюсера |
| `receiver` | `ReceiverInterface` | `new Receiver()` | Маршруты консьюмера, см. [Тестирование консьюмера](/ru/docs/2.x/consumer/testing) |

Передавайте `$bus` везде, где ваш код ожидает `BusInterface`.

### Драйверы проверок

Драйвер определяет, какой тестовый фреймворк получит проваленную проверку:

| Драйвер | Фреймворк |
|---|---|
| `PHPUnitAssertionDriver` | PHPUnit и Pest — бросает `ExpectationFailedException` |
| `TestoAssertionDriver` | Testo — бросает `AssertionException` из Testo |

Для другого фреймворка реализуйте `AssertionDriverInterface` — два метода, `notEmpty()` и `count()`.

## Проверка опубликованных сообщений

```php
$bus->publish(new ProductCreatedMessage(1, 'Laptop', 'Electronics'));

$bus->assertPublished(ProductCreatedMessage::class);
$bus->assertPublishedTimes(ProductCreatedMessage::class, 1);
```

### Проверка с условием

Колбэк получает `ProducerMessage` — результат всего pipeline, включая сериализацию и заголовки, добавленные middleware:

```php
use KafkaBus\Core\Producers\Messages\ProducerMessage;

$bus->assertPublished(
    ProductCreatedMessage::class,
    static fn (ProducerMessage $message): bool => $message->key === '1'
        && $message->headers['event'] === 'product.created',
);
```

Доступные свойства `ProducerMessage` (все `readonly`):

| Свойство | Тип | Описание |
|---|---|---|
| `topic` | `Topic` | Целевой топик |
| `payload` | `string` | Результат `toPayload()` |
| `headers` | `array<string, mixed>` | Итоговые заголовки |
| `partition` | `int` | `-1`, если партиция не задана явно |
| `key` | `?string` | Ключ партиционирования |

### Проверка, что ничего не опубликовано

```php
$bus->assertNotPublished(ProductCreatedMessage::class);
$bus->assertNothingPublished();
```

### Получение опубликованных сообщений

```php
$messages = $bus->getPublished(ProductCreatedMessage::class); // list<ProducerMessage>
$all = $bus->allPublished();                                   // все топики
```

::: warning Сообщения группируются по топику
`getPublished()` определяет топик по классу сообщения через таблицу маршрутов и возвращает все сообщения этого топика. Если в один топик идут несколько классов сообщений, в результат попадут все.

Для класса без маршрута метод возвращает пустой массив — поэтому `assertNotPublished()` для него всегда проходит.
:::

## Проверка middleware

`BusFaker` записывает сообщения после pipeline, поэтому middleware проверяются без дополнительной настройки — добавьте их в `Options` маршрута:

```php
use KafkaBus\Commiter\Middleware\PublisherIdempotencyMiddleware;
use KafkaBus\Core\Publishers\Routing\Options;

$routes = PublisherRoutesBuilder::make($topicRegistry)
    ->add(OrderMessage::class, 'orders', new Options(
        middleware: [new PublisherIdempotencyMiddleware()],
    ))
    ->build();

$bus = BusFaker::make($topicRegistry, new PHPUnitAssertionDriver(), new PublisherFactory(routes: $routes));

$bus->publish(new OrderMessage(orderId: 42));

$bus->assertPublished(
    OrderMessage::class,
    static fn (ProducerMessage $message): bool => $message->headers['x-idempotency-key'] === 'order-42',
);
```

Отмена отправки проверяется так же: если middleware не вызвал `continue()`, проходит `assertNotPublished()`.

## Тестовый класс сообщения

`ProducerMessageFaker` — облегчённая реализация `ProducerMessageInterface` для тестов. Реализует `HasHeaders` и `HasPartition`:

```php
use KafkaBus\Core\Testing\Messages\ProducerMessageFaker;

$message = new ProducerMessageFaker(
    message: 'test-message',
    headers: ['foo' => 'bar'],
    partition: 5,
);
```

| Параметр | Тип | По умолчанию | Описание |
|---|---|---|---|
| `message` | `string` | — | Сырой payload, возвращаемый `toPayload()` |
| `headers` | `array` | `[]` | Заголовки, возвращаемые `getHeaders()` |
| `partition` | `int` | `-1` | Номер партиции; `-1` — выбор за Kafka |

`HasKey` он не реализует, поэтому ключ всегда `null`. В реальных тестах лучше использовать собственные классы сообщений — тогда тест покрывает и сериализацию.

## Низкоуровневый доступ

`BusFaker` построен на `ConnectionFaker`, который хранит опубликованные сообщения в публичном массиве с ключом по физическому имени топика. Прочитать его можно через подключение фейка:

```php
/** @var \KafkaBus\Core\Testing\Connections\ConnectionFaker $connection */
$connection = $bus->connection();

$connection->publishedMessages['production.fact.products.1']; // list<ProducerMessage>
```
