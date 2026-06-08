# Producer

Producer отвечает за отправку сообщений в Kafka. На уровне ядра это `Bus::publish()`, которому передаётся объект, реализующий `ProducerMessageInterface`.

## ProducerMessage

Базовый класс для исходящих сообщений:

```php
use KafkaBus\Core\Producers\Messages\ProducerMessage;

$bus->publish(new ProducerMessage(
    payload: json_encode(['id' => 1, 'name' => 'Laptop']),
    headers: ['source' => 'catalog-service', 'version' => '1'],
    key:     '1', // Kafka partition key (опционально)
));
```

## Собственные классы сообщений

Реализуйте `ProducerMessageInterface`, чтобы инкапсулировать логику сериализации:

```php
use KafkaBus\Core\Interfaces\Producers\Messages\ProducerMessageInterface;
use KafkaBus\Core\Interfaces\Producers\Messages\HasKey;
use KafkaBus\Core\Interfaces\Producers\Messages\HasHeaders;

final readonly class ProductCreatedMessage implements ProducerMessageInterface, HasKey, HasHeaders
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

## Batch-публикация

`Bus::publishBatch()` можно отправить сразу несколько сообщений:

```php
namespace KafkaBus\Core\Bus\MessageBatch;

$messages = [];

foreach ($products as $product) {
    $messages[] = new ProductCreatedMessage($product->id, $product->name);
}

$bus->publishBatch(MessageBatch::fromArray($messages));
```

## Переключение соединения

```php
$bus->onConnection('analytics')->publish($message);
```
