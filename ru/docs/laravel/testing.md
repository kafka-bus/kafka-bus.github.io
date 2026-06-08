# Тестирование

Фасад `KafkaBus` поставляется с полноценной fake-заглушкой, работающей аналогично `Event::fake()` или `Mail::fake()`.

## Активация fake

```php
use KafkaBus\Laravel\Facades\KafkaBus;

KafkaBus::fake();
```

После этого все вызовы через Facade (`publish`, `listen`) перехватываются in-memory fake. Реальный брокер не используется.

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
    fn($msg) => str_contains($msg->payload, '"id":1')
             && isset($msg->headers['x-idempotency-key'])
);
```

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

// Все опубликованные сообщения
$all = KafkaBus::allPublished();

expect($messages[0]->payload)->toContain('"id":1');
expect($messages[0]->headers)->toHaveKey('x-idempotency-key');
```

## Тестирование Consumer

`addMessage()` добавляет `RdKafka\Message` в fake подключение, а `listen()` запускает полный pipeline консьюмера — middleware, маршрутизацию, обработчик, коммит — без реального брокера.

### Создание тестового сообщения

```php
use Micromus\KafkaBus\Testing\Consumers\MessageFactory;

$message = MessageFactory::for()
    ->withTopicKey('products')
    ->withHeaders(['x-idempotency-key' => 'prod-42'])
    ->make('{"id":42,"name":"Ноутбук"}');
```

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

### Несколько сообщений

```php
$factory = MessageFactory::for()->withTopicKey('products');

KafkaBus::addMessage($factory->make('{"id":1}'));
KafkaBus::addMessage($factory->make('{"id":2}'));
KafkaBus::addMessage($factory->make('{"id":3}'));

KafkaBus::listen('products');

expect(Product::count())->toBe(3);
```

## Проверка коммитов

После `listen()` каждое успешно обработанное сообщение записывается в fake. Это позволяет проверить, что обработчик отработал и offset был подтверждён:

```php
KafkaBus::listen('products');

// Хотя бы одно сообщение было закоммичено
KafkaBus::assertCommitted('products');

// Проверка с условием
KafkaBus::assertCommitted(
    'products',
    fn($msg) => $msg->payload() === '{"id":1}'
             && $msg->headers()['x-idempotency-key'] === 'prod-42'
);

// Точное количество
KafkaBus::assertCommittedTimes('products', 2);

// Ничего не закоммичено (например, до вызова listen)
KafkaBus::assertNothingCommitted();
```

### Получение закоммиченных сообщений

```php
$committed = KafkaBus::getCommitted('products'); // list<ConsumerMessageInterface>

expect($committed[0]->payload())->toBe('{"id":42}');
expect($committed[0]->headers())->toHaveKey('x-idempotency-key');
```

## Тестовое окружение (.env.testing)

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

С `KafkaBus::fake()` подключение не имеет значения — fake перехватывает вызовы до того, как они достигают его. Драйвер `null` полезен, когда fake не активирован, но тесты всё равно не должны подключаться к реальному Kafka.
