# Тестирование

`KafkaBus` Facade поставляется с первоклассным fake, который работает аналогично `Event::fake()` или `Mail::fake()`.

## Активация фейка

```php
use KafkaBus\Laravel\Facades\KafkaBus;

KafkaBus::fake();
```

После этого все вызовы через Facade (`publish`, `listen`) перехватываются in-memory фейком. Реальный брокер не используется.

## Проверка публикации сообщений

### Базовая проверка

```php
it('публикует сообщение при создании продукта', function () {
    KafkaBus::fake();

    app(CreateProductAction::class)->execute(id: 1, name: 'Laptop');

    KafkaBus::assertPublished(ProductMessage::class);
});
```

### Проверка с условием

Callback получает `ProducerMessage` после полного прохождения middleware-конвейера (включая сериализацию payload и добавление заголовков):

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

### Проверка отсутствия публикации

```php
KafkaBus::assertNotPublished(ProductMessage::class);
KafkaBus::assertNothingPublished();
```

### Получение опубликованных сообщений

```php
// Все сообщения одного типа (после middleware, с payload и headers)
$messages = KafkaBus::getPublished(ProductMessage::class);

// Все опубликованные сообщения
$all = KafkaBus::allPublished();

expect($messages[0]->payload)->toContain('"id":1');
expect($messages[0]->headers)->toHaveKey('x-idempotency-key');
```

## Тестирование consumer'а

`addMessage()` добавляет `RdKafka\Message` в фейковое соединение, `listen()` запускает полный consumer-конвейер — middleware, маршрутизацию, обработчик, коммит — без реального брокера.

### Создание тестового сообщения

```php
use Micromus\KafkaBus\Testing\Consumers\MessageFactory;

$message = MessageFactory::for()
    ->withTopicKey('products')
    ->withHeaders(['x-idempotency-key' => 'prod-42'])
    ->make('{"id":42,"name":"Laptop"}');
```

### Полный тест consumer'а

```php
it('сохраняет продукт при получении сообщения', function () {
    KafkaBus::fake();

    KafkaBus::addMessage(
        MessageFactory::for()
            ->withTopicKey('products')
            ->withHeaders(['x-idempotency-key' => 'prod-42'])
            ->make('{"id":42,"name":"Laptop","status":"active"}')
    );

    KafkaBus::listen('products');

    expect(Product::find(42))->not->toBeNull();
    expect(Product::find(42)->name)->toBe('Laptop');
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

После `listen()` каждое успешно обработанное сообщение фиксируется в фейке. Это позволяет проверить, что обработчик отработал и офсет был подтверждён:

```php
KafkaBus::listen('products');

// Хотя бы одно сообщение закоммичено
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

Настройте `null`-соединение для запуска тестов без брокера:

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

При `KafkaBus::fake()` соединение не важно — фейк перехватывает вызовы раньше. `null`-драйвер полезен, когда фейк не активирован, но тесты всё равно не должны ходить в реальный Kafka.
