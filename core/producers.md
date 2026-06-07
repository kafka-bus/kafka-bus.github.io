# Producer

Producer отвечает за отправку сообщений в Kafka. На уровне ядра это `Bus::publish()`, которому передаётся объект, реализующий `ProducerMessageInterface`.

## ProducerMessage

Базовый класс для исходящих сообщений:

```php
use Micromus\KafkaBus\Producers\Messages\ProducerMessage;

$bus->publish(new ProducerMessage(
    payload: json_encode(['id' => 1, 'name' => 'Laptop']),
    headers: ['source' => 'catalog-service', 'version' => '1'],
    key:     '1', // Kafka partition key (опционально)
));
```

## Собственные классы сообщений

Реализуйте `ProducerMessageInterface`, чтобы инкапсулировать логику сериализации:

```php
use Micromus\KafkaBus\Interfaces\Producers\Messages\ProducerMessageInterface;

final readonly class ProductCreatedMessage implements ProducerMessageInterface
{
    public function __construct(
        private int    $id,
        private string $name,
        private string $category,
    ) {}

    public function toPayload(): string
    {
        return json_encode([
            'id'       => $this->id,
            'name'     => $this->name,
            'category' => $this->category,
        ]);
    }

    public function getKey(): ?string
    {
        return (string) $this->id; // Гарантирует, что все события одного продукта попадут в одну партицию
    }

    public function getHeaders(): array
    {
        return ['event' => 'product.created'];
    }
}

$bus->publish(new ProductCreatedMessage(1, 'Laptop', 'Electronics'));
```

## Маршруты publisher

`PublisherRoutesBuilder` связывает класс сообщения с логическим ключом топика:

```php
use Micromus\KafkaBus\Bus\Publishers\Router\PublisherRoutesBuilder;

$publisherRoutes = PublisherRoutesBuilder::make($topicRegistry)
    ->add(ProductCreatedMessage::class, 'products')
    ->add(OrderCreatedMessage::class,   'orders')
    ->build();
```

### Маршруты с опциями

Для каждого маршрута можно задать индивидуальные опции и middleware:

```php
use Micromus\KafkaBus\Bus\Publishers\Router\Options;

$publisherRoutes = PublisherRoutesBuilder::make($topicRegistry)
    ->add(
        messageClass: ProductCreatedMessage::class,
        topicKey:     'products',
        options:      new Options(
            middleware:        [new AuditMiddleware()],
            additionalOptions: ['compression.codec' => 'gzip'],
            flushTimeout:      10_000,
            flushRetries:      3,
        )
    )
    ->build();
```

## Flush

После отправки сообщений `rdkafka` асинхронно доставляет их в брокер. `flushTimeout` и `flushRetries` контролируют ожидание подтверждения:

| Параметр | По умолчанию | Описание |
|---|---|---|
| `flushTimeout` | `5000` мс | Таймаут одной попытки flush |
| `flushRetries` | `5` | Число попыток перед ошибкой |

::: warning
Если процесс завершится до flush, сообщения могут не дойти до брокера. Для критичных сообщений используйте [`kafka-bus-outbox`](/packages/outbox).
:::

## Batch-публикация

`Bus::publish()` можно вызывать несколько раз подряд — сообщения накапливаются в батч и отправляются вместе:

```php
foreach ($products as $product) {
    $bus->publish(new ProductCreatedMessage($product->id, $product->name));
}
// Flush происходит автоматически после исчерпания батча
```

## Переключение соединения

```php
$bus->onConnection('analytics')->publish($message);
```
