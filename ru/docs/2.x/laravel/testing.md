---
title: Тестирование в Laravel
description: Подмена шины Kafka Bus в тестах Laravel — проверка публикаций, передача сообщений, запуск воркеров и проверка коммитов
---

# Тестирование

Фасад `KafkaBus` поставляется с фейком, который работает как `Event::fake()` или `Mail::fake()`. Он подменяет только транспорт: маршруты, топики, обработчики и middleware остаются настоящими.

## Подмена шины

```php
use KafkaBus\Laravel\Facades\KafkaBus;

KafkaBus::fake();
```

Вызов создаёт `FakeBus` поверх [`BusFaker`](/ru/docs/2.x/producer/testing) из ядра и регистрирует его в контейнере как `BusInterface`. Фейковое подключение регистрируется как `ConnectionInterface`. Проваленные проверки сообщаются через PHPUnit, поэтому работают и в PHPUnit, и в Pest.

::: tip Работает и с внедрением зависимостей
Подменяется сама привязка `BusInterface`, поэтому классы, получающие `BusInterface` через конструктор, тоже получат фейк.

Вызывайте `KafkaBus::fake()` до того, как тестируемый сервис будет получен из контейнера.
:::

## Проверка опубликованных сообщений

```php
it('publishes a message when a product is created', function () {
    KafkaBus::fake();

    app(CreateProductAction::class)->execute(id: 1, name: 'Laptop');

    KafkaBus::assertPublished(ProductMessage::class);
});
```

### Проверка с условием

Колбэк получает `ProducerMessage` после всего pipeline — с сериализованным payload и добавленными заголовками:

```php
KafkaBus::assertPublished(
    ProductMessage::class,
    fn ($message) => str_contains($message->payload, '"id":1')
        && isset($message->headers['x-idempotency-key'])
);
```

Доступные свойства `ProducerMessage`: `topic`, `payload`, `headers`, `partition`, `key` — все `readonly`.

### Другие проверки публикации

```php
KafkaBus::assertPublishedTimes(ProductMessage::class, 3);
KafkaBus::assertNotPublished(ProductMessage::class);
KafkaBus::assertNothingPublished();

$messages = KafkaBus::getPublished(ProductMessage::class); // list<ProducerMessage>
$all = KafkaBus::allPublished();
```

::: warning Сообщения группируются по топику
`getPublished()` определяет топик по классу сообщения через таблицу маршрутов и возвращает все сообщения этого топика. Если в один топик идут несколько классов сообщений, в результат попадут все.

Для класса без маршрута метод возвращает пустой массив — поэтому `assertNotPublished()` для него всегда проходит. Проверяйте маршруты командой `kafka:publisher:route:list`.
:::

## Тестирование обработчика

Передать сообщение в обработчик можно двумя способами:

- `KafkaBus::dispatch()` — сообщение идёт прямо в шину: маршрутизация, middleware, обработчик. Без воркера и без коммита;
- `KafkaBus::addMessage()` + `KafkaBus::listen()` — над очередью сообщений запускается настоящий цикл воркера, включая коммит.

### Создание тестового сообщения

```php
use KafkaBus\Core\Testing\Consumers\MessageFactory;

$message = MessageFactory::for()
    ->withTopicKey('products')
    ->withHeaders(['x-idempotency-key' => 'prod-42'])
    ->make('{"id":42,"name":"Laptop"}');
```

`withTopicKey()` принимает логический ключ — фейк сам превращает его в физическое имя с префиксом топиков. Полный список методов — в разделе [Тестирование консьюмера](/ru/docs/2.x/consumer/testing#создание-тестовых-сообщении).

### Передача сообщения в шину

```php
use KafkaBus\Core\Consumers\Messages\ConsumerMessage;

it('stores a product when the message is received', function () {
    KafkaBus::fake();

    KafkaBus::dispatch(new ConsumerMessage(
        MessageFactory::for()
            ->withTopicKey('products')
            ->make('{"id":42,"name":"Laptop","status":"active"}')
    ));

    expect(Product::find(42)?->name)->toBe('Laptop');

    KafkaBus::assertDispatched('products');
});
```

### Запуск воркера

```php
it('reads and commits product messages', function () {
    KafkaBus::fake();

    $factory = MessageFactory::for()->withTopicKey('products');

    KafkaBus::addMessage($factory->withOffset(0)->make('{"id":1}'));
    KafkaBus::addMessage($factory->withOffset(1)->make('{"id":2}'));

    KafkaBus::listen('products');

    expect(Product::count())->toBe(2);

    KafkaBus::assertCommittedTimes('products', 2);
});
```

`listen()` принимает имя воркера — ключ из `consumers.workers` или ключ топика маршрута — либо список имён. Он сам возвращает управление, когда сообщения в очереди закончатся.

::: warning
Вызывайте `addMessage()` до `listen()`: каждый `listen()` берёт снимок очереди в момент запуска.
:::

::: tip
Задавайте разные оффсеты сообщениям одного топика: `msgId()` строится как `"{partition}-{offset}-{topic}"`, и с `ConsumerCommiterMiddleware` сообщения с одинаковым оффсетом считаются дублями.
:::

### Несколько воркеров

```php
KafkaBus::addMessage(MessageFactory::for()->withTopicKey('products')->make('{"id":1}'));
KafkaBus::addMessage(MessageFactory::for()->withTopicKey('orders')->make('{"id":2}'));

KafkaBus::listen(['products', 'orders']);

KafkaBus::assertCommitted('products');
KafkaBus::assertCommitted('orders');
```

Топики перечисленных воркеров объединяются в один поток консьюмера — так же, как в `kafka:consume products orders`. Каждому топику в очереди нужен маршрут; сообщение без маршрута останавливает `listen()` исключением `MessageConsumerNotHandledException`.

### Ошибки обработчика

```php
use KafkaBus\Core\Exceptions\Consumers\MessageConsumerNotHandledException;

it('does not commit a message when the handler fails', function () {
    KafkaBus::fake();

    KafkaBus::addMessage(
        MessageFactory::for()->withTopicKey('products')->make('{"broken":true}')
    );

    expect(fn () => KafkaBus::listen('products'))
        ->toThrow(MessageConsumerNotHandledException::class);

    KafkaBus::assertNothingCommitted();
});
```

### Artisan-команды под фейком

`kafka:consume` и `kafka:consume:routes` получают подменённую шину, поэтому читают сообщения из очереди и завершаются, когда она пуста:

```php
KafkaBus::fake();

KafkaBus::addMessage(MessageFactory::for()->withTopicKey('products')->make('{"id":1}'));

$this->artisan('kafka:consume:routes')->assertExitCode(0);

KafkaBus::assertCommitted('products');
```

## Проверки передачи и коммитов

Все эти методы принимают ключ топика, а не имя воркера, и резолвят его через `TopicRegistry`. Незарегистрированный ключ вызывает `TopicCannotResolvedException`.

```php
KafkaBus::assertDispatched('products');
KafkaBus::assertDispatched('products', fn ($message) => $message->payload() === '{"id":1}');
KafkaBus::assertDispatchedTimes('products', 2);
KafkaBus::assertNotDispatched('orders');
KafkaBus::assertNothingDispatched();

KafkaBus::assertCommitted('products');
KafkaBus::assertCommitted('products', fn ($message) => $message->headers()['x-idempotency-key'] === 'prod-42');
KafkaBus::assertCommittedTimes('products', 2);
KafkaBus::assertNothingCommitted();
```

Сообщение считается переданным после успешного выполнения обработчика и закоммиченным — после коммита воркером. Сам по себе `dispatch()` никогда не коммитит.

## Справочник методов

| Метод | Назначение |
|---|---|
| `fake()` | Включает фейк |
| `assertPublished($class, ?$callback)` | Сообщение опубликовано |
| `assertPublishedTimes($class, $times)` | Опубликовано ровно N раз |
| `assertNotPublished($class)` | Сообщение не публиковалось |
| `assertNothingPublished()` | Ничего не опубликовано |
| `getPublished($class)` | `list<ProducerMessage>` для класса |
| `allPublished()` | `list<ProducerMessage>` по всем топикам |
| `dispatch(ConsumerMessageInterface $message)` | Передаёт сообщение в шину |
| `addMessage(RdKafka\Message $message)` | Ставит сообщение в очередь для воркера |
| `listen(string\|array $workerName)` | Запускает воркер над очередью |
| `assertDispatched($topicKey, ?$callback)` | Сообщение передано в шину |
| `assertDispatchedTimes($topicKey, $times)` | Передано ровно N раз |
| `assertNotDispatched($topicKey)` | Для топика ничего не передано |
| `assertNothingDispatched()` | Ничего не передано |
| `getDispatched($topicKey)` | `list<ConsumerMessageInterface>` для топика |
| `allDispatched()` | Все переданные сообщения |
| `assertCommitted($topicKey, ?$callback)` | Сообщение закоммичено |
| `assertCommittedTimes($topicKey, $times)` | Закоммичено ровно N раз |
| `assertNothingCommitted()` | Ничего не закоммичено |
| `getCommitted($topicKey)` | `list<ConsumerMessageInterface>` для топика |

## Тестовое окружение

Настройте подключение `null`, чтобы тесты никогда не обращались к реальному брокеру:

```dotenv
# .env.testing
KAFKA_CONNECTION=testing
```

```php
// config/kafka-bus.php
'connections' => [
    'testing' => [
        'driver'  => 'null',
        'options' => [],
    ],
],
```

С `KafkaBus::fake()` подключение не важно — фейк его подменяет. Драйвер `null` защищает тесты, которые не вызывают `fake()`: публикации молча отбрасываются.

::: warning
Читать через подключение `null` нельзя: создание консьюмера бросает `ConsumerException`. Для тестов консьюмера всегда используйте `KafkaBus::fake()`.
:::
