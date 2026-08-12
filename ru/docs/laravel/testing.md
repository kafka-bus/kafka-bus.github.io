# Тестирование

Фасад `KafkaBus` поставляется с полноценной fake-заглушкой, работающей аналогично `Event::fake()` или `Mail::fake()`.

## Подмена шины в тестах

```php
use KafkaBus\Laravel\Facades\KafkaBus;

KafkaBus::fake();
```

Вызов подменяет в контейнере `ConnectionRegistryInterface` на `ConnectionRegistryFaker` и пересобирает `BusInterface`, после чего регистрирует получившийся `FakeBus` как singleton.

::: tip Работает и с DI
Подменяется именно привязка `BusInterface`, поэтому классы, принимающие `BusInterface` через конструктор, тоже получат fake — переписывать код под фасад не нужно.

Важно только вызвать `KafkaBus::fake()` **до** того, как тестируемый сервис будет разрешён из контейнера.
:::

Реальный брокер при этом не используется: маршруты, топики, pipeline и middleware остаются настоящими, подменяется только транспорт.

## Проверка опубликованных сообщений

### Базовая проверка

```php
it('публикует сообщение при создании продукта', function () {
    KafkaBus::fake();

    app(CreateProductAction::class)->execute(id: 1, name: 'Ноутбук');

    KafkaBus::assertPublished(ProductMessage::class);
});
```

### Проверка с условием

Callback получает `ProducerMessage` после прохождения через полный pipeline (включая сериализацию payload и добавление заголовков):

```php
KafkaBus::assertPublished(
    ProductMessage::class,
    fn ($msg) => str_contains($msg->payload, '"id":1')
              && isset($msg->headers['x-idempotency-key'])
);
```

Доступные свойства `ProducerMessage`: `topic`, `payload`, `headers`, `partition`, `key` — все `readonly`.

### Проверка количества

```php
KafkaBus::assertPublishedTimes(ProductMessage::class, 3);
```

### Проверка, что ничего не опубликовано

```php
KafkaBus::assertNotPublished(ProductMessage::class);
KafkaBus::assertNothingPublished();
```

### Получение опубликованных сообщений

```php
// Все сообщения указанного типа (после middleware, с payload и заголовками)
$messages = KafkaBus::getPublished(ProductMessage::class);

// Все опубликованные сообщения по всем топикам
$all = KafkaBus::allPublished();

expect($messages[0]->payload)->toContain('"id":1');
expect($messages[0]->headers)->toHaveKey('x-idempotency-key');
```

::: warning Сообщения группируются по топику
`getPublished()` находит топик по классу сообщения через таблицу маршрутов, а затем возвращает **все** сообщения этого топика. Если в один топик маршрутизировано несколько классов сообщений, они попадут в общую выборку.

Для класса без зарегистрированного маршрута метод вернёт пустой массив — а `assertNotPublished()` для него всегда пройдёт, даже если публикация была бы невозможна. Проверяйте маршруты командой `kafka:route:list`.
:::

## Тестирование консьюмера

`addMessage()` добавляет `RdKafka\Message` в fake-подключение, а `listen()` запускает полный pipeline консьюмера — middleware, маршрутизацию, обработчик, коммит — без реального брокера.

### Создание тестового сообщения

```php
use KafkaBus\Core\Testing\Consumers\MessageFactory;

$message = MessageFactory::for()
    ->withTopicKey('products')
    ->withHeaders(['x-idempotency-key' => 'prod-42'])
    ->make('{"id":42,"name":"Ноутбук"}');
```

`withTopicKey()` принимает как логический ключ, так и полное имя топика — `ConnectionFaker` резолвит его через `TopicRegistry`. Полный список методов — в разделе [Тестирование консьюмера](/ru/docs/consumer/testing#messagefactory).

### Полный тест консьюмера

```php
it('сохраняет продукт при получении сообщения', function () {
    KafkaBus::fake();

    KafkaBus::addMessage(
        MessageFactory::for()
            ->withTopicKey('products')
            ->withHeaders(['x-idempotency-key' => 'prod-42'])
            ->make('{"id":42,"name":"Ноутбук","status":"active"}')
    );

    KafkaBus::listen('products');

    expect(Product::find(42))->not->toBeNull();
    expect(Product::find(42)->name)->toBe('Ноутбук');
});
```

`listen()` принимает имя воркера (не топика) и завершается сам, когда сообщения в fake-подключении закончились, — блокировки не будет.

### Несколько сообщений

```php
$factory = MessageFactory::for()->withTopicKey('products');

KafkaBus::addMessage($factory->withOffset(0)->make('{"id":1}'));
KafkaBus::addMessage($factory->withOffset(1)->make('{"id":2}'));
KafkaBus::addMessage($factory->withOffset(2)->make('{"id":3}'));

KafkaBus::listen('products');

expect(Product::count())->toBe(3);
```

### Тестирование нескольких воркеров <Badge type="info" text="laravel-bridge ^1.2" />

Начиная с `kafka-bus/laravel-bridge:^1.2`, метод `listen()` принимает массив имён воркеров. Их топики объединяются в единый consumer stream — аналогично поведению `kafka:consume worker1 worker2`.

```php
it('обрабатывает сообщения из воркеров products и orders', function () {
    KafkaBus::fake();

    KafkaBus::addMessage(
        MessageFactory::for()->withTopicKey('products')->make('{"id":1}')
    );
    KafkaBus::addMessage(
        MessageFactory::for()->withTopicKey('orders')->make('{"id":2}')
    );

    KafkaBus::listen(['products', 'orders']);

    KafkaBus::assertCommitted('products');
    KafkaBus::assertCommitted('orders');
});
```

::: tip
Задавайте разные `offset`: `msgId()` собирается как `"{partition}-{offset}-{topic}"`, и при активном `ConsumerCommiterMiddleware` сообщения с одинаковым offset будут приняты за дубликаты и пропущены.
:::

### Сообщения для нескольких топиков

Fake-подключение хранит одну общую очередь. Сообщения разных топиков можно смешивать — маршрутизация разложит их по обработчикам:

```php
KafkaBus::addMessage(MessageFactory::for()->withTopicKey('products')->make('{"id":1}'));
KafkaBus::addMessage(MessageFactory::for()->withTopicKey('orders')->make('{"id":2}'));

KafkaBus::listen('default'); // воркер, подписанный на оба топика
```

::: warning
Воркер должен иметь маршрут для каждого топика в очереди. Сообщение без маршрута вызовет `RouteConsumerException`.
:::

### Тестирование ошибок обработчика

```php
it('не коммитит сообщение при ошибке обработчика', function () {
    KafkaBus::fake();

    KafkaBus::addMessage(
        MessageFactory::for()->withTopicKey('products')->make('{"broken":true}')
    );

    expect(fn () => KafkaBus::listen('products'))
        ->toThrow(MessageConsumerNotHandledException::class);

    KafkaBus::assertNothingCommitted();
});
```

## Проверка коммитов

После `listen()` каждое успешно обработанное сообщение записывается в fake. Это позволяет проверить, что обработчик отработал и offset был подтверждён:

```php
KafkaBus::listen('products');

// Хотя бы одно сообщение было закоммичено
KafkaBus::assertCommitted('products');

// Проверка с условием — callback получает ConsumerMessageInterface
KafkaBus::assertCommitted(
    'products',
    fn ($msg) => $msg->payload() === '{"id":1}'
              && $msg->headers()['x-idempotency-key'] === 'prod-42'
);

// Точное количество
KafkaBus::assertCommittedTimes('products', 2);

// Ничего не закоммичено (например, до вызова listen)
KafkaBus::assertNothingCommitted();
```

Все методы для коммитов принимают **ключ топика**, а не имя воркера, и резолвят его через `TopicRegistry`. Незарегистрированный ключ вызовет `TopicCannotResolvedException`.

### Получение закоммиченных сообщений

```php
$committed = KafkaBus::getCommitted('products'); // list<ConsumerMessageInterface>

expect($committed[0]->payload())->toBe('{"id":42}');
expect($committed[0]->headers())->toHaveKey('x-idempotency-key');
expect($committed[0]->original()->offset)->toBe(0);
```

## Методы `FakeBus`

| Метод                                      | Назначение                                     |
|--------------------------------------------|------------------------------------------------|
| `fake()`                                   | Активирует заглушку                            |
| `assertPublished($class, ?$callback)`      | Сообщение опубликовано                         |
| `assertPublishedTimes($class, $times)`     | Опубликовано ровно N раз                       |
| `assertNotPublished($class)`               | Сообщение не публиковалось                     |
| `assertNothingPublished()`                 | Ничего не публиковалось                        |
| `getPublished($class)`                     | `list<ProducerMessage>` для класса             |
| `allPublished()`                           | `list<ProducerMessage>` по всем топикам        |
| `addMessage(RdKafka\Message $message)`     | Кладёт сообщение в очередь чтения              |
| `listen(string $workerName)`               | Прогоняет pipeline консьюмера                  |
| `assertCommitted($topicKey, ?$callback)`   | Сообщение закоммичено                          |
| `assertCommittedTimes($topicKey, $times)`  | Закоммичено ровно N раз                        |
| `assertNothingCommitted()`                 | Ничего не коммитилось                          |
| `getCommitted($topicKey)`                  | `list<ConsumerMessageInterface>` для топика    |

## Тестовое окружение

Настройте `null` подключение для запуска тестов без брокера:

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

С `KafkaBus::fake()` подключение не имеет значения — fake перехватывает вызовы до того, как они достигают его. Драйвер `null` полезен, когда fake не активирован, но тесты всё равно не должны подключаться к реальному Kafka: публикации будут молча отброшены.

::: warning
На `null`-подключении нельзя *читать*: создание консьюмера бросит `ConsumerException`. Для тестов консьюмера всегда используйте `KafkaBus::fake()`.
:::
