---
title: Тестирование консьюмера
description: Тестирование обработчиков, middleware и воркеров без брокера Kafka с помощью BusFaker и MessageFactory
---

# Тестирование

В `kafka-bus/core` есть `BusFaker` и `MessageFactory`, которые позволяют прогнать сторону консьюмера без реального брокера Kafka. Тестировать можно на двух уровнях:

- **через dispatch** — передайте сообщение прямо в `Bus::dispatch()` и проверьте обработчик; быстро и без воркеров;
- **через воркер** — поставьте сообщения в очередь фейкового подключения и запустите над ними настоящий цикл воркера; проверяет топики, маршрутизацию и коммиты вместе.

::: tip Laravel
В Laravel-приложении используйте `KafkaBus::fake()`, `KafkaBus::addMessage()`, `KafkaBus::dispatch()` и `KafkaBus::listen()`. См. [Тестирование в Laravel](/ru/docs/2.x/laravel/testing).
:::

## Создание тестовых сообщений

`MessageFactory` создаёт объекты `RdKafka\Message`. Это иммутабельный билдер — каждый вызов `with*` возвращает клон, поэтому один экземпляр фабрики может порождать множество вариантов.

```php
use KafkaBus\Core\Testing\Consumers\MessageFactory;

$message = MessageFactory::for()
    ->withTopicKey('products', $topicRegistry)
    ->withHeaders(['x-idempotency-key' => 'prod-42'])
    ->make('{"id":42,"name":"Laptop"}');
```

| Метод | По умолчанию | Описание |
|---|---|---|
| `withTopicKey(string $key, ?TopicRegistry $registry = null)` | `'test'` | Задаёт `topic_name`; с реестром ключ превращается в физическое имя |
| `withHeaders(array $headers)` | `[]` | Задаёт заголовки сообщения |
| `withKey(?string $key)` | `null` | Задаёт ключ партиционирования |
| `withPartition(int $partition)` | `0` | Задаёт номер партиции |
| `withOffset(int $offset)` | `0` | Задаёт оффсет |
| `make(string $payload)` | — | Возвращает настроенный `RdKafka\Message` |
| `fromArray(array $attributes)` | — | Кодирует массив в JSON и вызывает `make()` |

У созданного так сообщения всегда `err = RD_KAFKA_RESP_ERR_NO_ERROR`.

::: tip Когда передавать реестр
`BusFaker::addMessage()` и `BusFaker::dispatch()` сами резолвят логические ключи, поэтому для них достаточно `withTopicKey('products')`. Передавайте реестр, когда отправляете сообщение в обычный `Bus` или `Receiver`, — маршрутизация идёт по физическому имени топика.
:::

## Тест обработчика через dispatch

Соберите ресивер так же, как в продакшене, и передайте его в `BusFaker::make()`:

```php
use KafkaBus\Core\Consumers\Messages\ConsumerMessage;
use KafkaBus\Core\Consumers\Messages\ConsumerMessageInterface;
use KafkaBus\Core\Receivers\Routing\ReceiverBuilder;
use KafkaBus\Core\Receivers\Routing\RouteInfo;
use KafkaBus\Core\Testing\Assertions\PHPUnitAssertionDriver;
use KafkaBus\Core\Testing\BusFaker;
use KafkaBus\Core\Testing\Consumers\MessageFactory;
use KafkaBus\Core\Topics\Topic;
use KafkaBus\Core\Topics\TopicRegistry;

$topicRegistry = (new TopicRegistry())
    ->add(new Topic('production.fact.products.1', 'products'));

$receiver = ReceiverBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', new ProductHandler()))
    ->build();

$bus = BusFaker::make($topicRegistry, new PHPUnitAssertionDriver(), receiver: $receiver);

$bus->dispatch(new ConsumerMessage(
    MessageFactory::for()
        ->withTopicKey('products')
        ->make('{"id":42,"name":"Laptop"}'),
));

$bus->assertDispatched('products');
$bus->assertDispatched(
    'products',
    static fn (ConsumerMessageInterface $message): bool => $message->payload() === '{"id":42,"name":"Laptop"}',
);
```

Сам по себе dispatch никогда не коммитит оффсет — после него проходит `assertNothingCommitted()`.

### Проверки передачи в шину

Все методы принимают ключ топика и резолвят его через `TopicRegistry`:

| Метод | Описание |
|---|---|
| `assertDispatched($topicKey, ?$callback)` | Передано хотя бы одно сообщение (подходящее под колбэк) |
| `assertDispatchedTimes($topicKey, $times)` | Передано ровно N сообщений |
| `assertNotDispatched($topicKey)` | Для топика ничего не передано |
| `assertNothingDispatched()` | Ничего не передано вообще |
| `getDispatched($topicKey)` | `list<ConsumerMessageInterface>` для топика |
| `allDispatched()` | Все переданные сообщения |

Сообщение считается переданным только после успешного выполнения обработчика.

## Тест цикла воркера

`addMessage()` ставит сообщение в очередь фейкового подключения. Воркер, созданный через `WorkerRunnerFactory`, читает очередь, передаёт каждое сообщение в шину и коммитит его:

```php
use KafkaBus\Worker\Registry\MemoryWorkerRegistry;
use KafkaBus\Worker\Worker;
use KafkaBus\Worker\WorkerRunnerFactory;

$bus->addMessage(MessageFactory::for()->withTopicKey('products')->make('{"id":1}'));
$bus->addMessage(MessageFactory::for()->withTopicKey('products')->withOffset(1)->make('{"id":2}'));

$workerRegistry = MemoryWorkerRegistry::make()
    ->add(new Worker('products', [$topicRegistry->get('products')]));

(new WorkerRunnerFactory($workerRegistry))
    ->create($bus, 'products')
    ->run();

$bus->assertCommittedTimes('products', 2);
```

::: tip Почему run() не зависает
Фейковый консьюмер отдаёт сообщения из заранее заполненной очереди. Когда она пуста, цикл корректно завершается, поэтому в тестах `run()` возвращает управление сам.
:::

::: warning
Фейковый консьюмер копирует очередь в момент создания — в `WorkerRunnerFactory::create()`. Вызывайте `addMessage()` до `create()`: сообщения, добавленные позже, этот раннер не прочитает.
:::

### Проверки коммитов

| Метод | Описание |
|---|---|
| `assertCommitted($topicKey, ?$callback)` | Закоммичено хотя бы одно сообщение (подходящее под колбэк) |
| `assertCommittedTimes($topicKey, $times)` | Закоммичено ровно N сообщений |
| `assertNothingCommitted()` | Ничего не закоммичено |
| `getCommitted($topicKey)` | `list<ConsumerMessageInterface>` для топика |

Сообщение коммитится только после успешного `dispatch()`, поэтому проверка коммитов — надёжный признак того, что вся цепочка отработала.

## Тестирование ошибок обработчика

Исключение обработчика выходит из `dispatch()` — и из `run()` воркера — обёрнутым в `MessageConsumerNotHandledException`:

```php
use KafkaBus\Core\Exceptions\Consumers\MessageConsumerNotHandledException;

$receiver = ReceiverBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', fn (string $payload) => throw new RuntimeException('boom')))
    ->build();

$bus = BusFaker::make($topicRegistry, new PHPUnitAssertionDriver(), receiver: $receiver);
$bus->addMessage(MessageFactory::for()->withTopicKey('products')->make('payload'));

try {
    (new WorkerRunnerFactory($workerRegistry))->create($bus, 'products')->run();
    $this->fail('Ожидалось исключение');
} catch (MessageConsumerNotHandledException $exception) {
    $this->assertInstanceOf(RuntimeException::class, $exception->getPrevious());
}

$bus->assertNothingCommitted();
```

Обратный сценарий проверяется так же: добавьте middleware, который глушит исключение, и убедитесь, что сообщение закоммичено.

## Тестирование middleware

Подключайте middleware к маршруту так же, как в продакшене, — дополнительная настройка не нужна:

```php
$receiver = ReceiverBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', new ProductHandler(), [new ErrorHandlingMiddleware($logger)]))
    ->build();
```

## Тестовые обработчики

В пространстве имён `KafkaBus\Core\Testing\Messages` есть два готовых обработчика для маршрутов, где сам обработчик не важен:

| Класс | Аргумент | Поведение |
|---|---|---|
| `VoidConsumerHandlerFaker` | `ConsumerMessageInterface` | Ничего не делает |
| `ConsumerHandlerFaker` | `string` | Печатает payload |
