# Тестирование

Пакет поставляется с фейковыми реализациями для изоляции тестов от реального брокера Kafka.

## NullConnection

Самый простой способ — подменить соединение на `null`-драйвер, который молча игнорирует все вызовы:

```php
use Micromus\KafkaBus\Connections\Config\NullConnectionConfig;
use Micromus\KafkaBus\Connections\Registry\ConnectionRegistry;

$connectionRegistry = new ConnectionRegistry(
    driverRegistry: $driverRegistry,
    connections: ['testing' => new NullConnectionConfig()],
    defaultConnectionName: 'testing',
);
```

Подходит для интеграционных тестов, где нужно убедиться, что код не падает, но проверять содержимое сообщений не нужно.

## ProducerFaker

`ProducerFaker` перехватывает все `Bus::publish()` и сохраняет отправленные сообщения в памяти для последующих утверждений:

```php
use Micromus\KafkaBus\Testing\Producers\ProducerFaker;

$faker = new ProducerFaker();

// Подставляем faker в Bus вместо реального producer
$bus = buildBusWithFaker($faker);

$bus->publish(new ProductCreatedMessage(id: 1, name: 'Laptop'));

// Проверяем, что сообщение было отправлено
$faker->assertPublished(ProductCreatedMessage::class);

// Проверяем количество
$faker->assertPublishedTimes(ProductCreatedMessage::class, 1);

// Проверяем, что ничего не было отправлено
$faker->assertNothingPublished();

// Проверяем с условием
$faker->assertPublished(
    ProductCreatedMessage::class,
    fn($msg) => str_contains($msg->payload, '"id":1')
);
```

## ConsumerFaker

`ConsumerFaker` позволяет имитировать получение сообщений из Kafka и тестировать обработчики без реального брокера:

```php
use Micromus\KafkaBus\Testing\Consumers\ConsumerFaker;
use Micromus\KafkaBus\Testing\Consumers\MessageFactory;

$faker = new ConsumerFaker();

// Создаём тестовое сообщение
$message = MessageFactory::for()
    ->withTopicKey('products')
    ->withHeaders(['x-source' => 'test'])
    ->make('{"id":1,"name":"Laptop"}');

// Помещаем сообщение в очередь
$faker->addMessage($message);

// Запускаем полный consumer-конвейер
$faker->listen('products');

// После listen() можно проверить коммиты
$faker->assertCommitted('products');
$faker->assertCommittedTimes('products', 1);
$faker->assertNothingCommitted();
```

## MessageFactory

`MessageFactory` используется для создания тестовых `RdKafka\Message` с удобным fluent API:

```php
use Micromus\KafkaBus\Testing\Consumers\MessageFactory;

$message = MessageFactory::for()
    ->withTopicKey('products')               // логический ключ топика
    ->withHeaders(['x-idempotency-key' => 'abc-123'])
    ->withKey('partition-key')               // Kafka partition key
    ->make('{"id":1,"name":"Widget"}');      // payload

// Несколько сообщений
$factory = MessageFactory::for()->withTopicKey('products');

$messages = [
    $factory->make('{"id":1}'),
    $factory->make('{"id":2}'),
    $factory->make('{"id":3}'),
];
```

## Пример теста (PHPUnit)

```php
use PHPUnit\Framework\TestCase;
use Micromus\KafkaBus\Testing\Consumers\MessageFactory;

class ProductConsumerTest extends TestCase
{
    public function test_handler_is_invoked_with_correct_data(): void
    {
        [$bus, $faker] = $this->buildTestBus();

        $faker->addMessage(
            MessageFactory::for()
                ->withTopicKey('products')
                ->make('{"id":42,"name":"Laptop"}')
        );

        $faker->listen('products');

        $faker->assertCommitted('products');
    }
}
```

## Пример теста (Pest)

```php
use Micromus\KafkaBus\Testing\Consumers\MessageFactory;

it('обрабатывает входящее сообщение о продукте', function () {
    [$bus, $faker] = buildTestBus();

    $faker->addMessage(
        MessageFactory::for()
            ->withTopicKey('products')
            ->withHeaders(['x-idempotency-key' => 'prod-42'])
            ->make('{"id":42,"name":"Laptop"}')
    );

    $faker->listen('products');

    $faker->assertCommitted(
        'products',
        fn($msg) => str_contains($msg->payload(), '"id":42')
    );
});
```

::: tip Laravel
В Laravel-пакете всё упрощено через `KafkaBus::fake()`. Подробнее — в разделе [Тестирование Laravel](/laravel/testing).
:::
