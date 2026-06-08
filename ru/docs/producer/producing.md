# Producer

Producer отвечает за отправку сообщений в Kafka. На уровне ядра это `Bus::publish()`, который принимает объект, реализующий `ProducerMessageInterface`.

## ProducerMessage

Базовый класс для исходящих сообщений:

```php
use KafkaBus\Core\Producers\Messages\ProducerMessage;

$bus->publish(new ProducerMessage(
    payload: json_encode(['id' => 1, 'name' => 'Ноутбук']),
    headers: ['source' => 'catalog-service', 'version' => '1'],
    key:     '1', // ключ партиции Kafka (необязательно)
));
```

## Собственные классы сообщений

Реализуйте `ProducerMessageInterface` для инкапсуляции логики сериализации:

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
        return (string) $this->id; // все события одного продукта идут в одну партицию
    }

    public function getHeaders(): array
    {
        return ['event' => 'product.created'];
    }
}

$bus->publish(new ProductCreatedMessage(1, 'Ноутбук', 'Электроника'));
```

## Пакетная публикация

Используйте `Bus::publishBatch()` для отправки нескольких сообщений за один вызов:

```php
namespace KafkaBus\Core\Bus\MessageBatch;

$messages = [];

foreach ($products as $product) {
    $messages[] = new ProductCreatedMessage($product->id, $product->name);
}

$bus->publishBatch(MessageBatch::fromArray($messages));
```

## Переключение подключений

```php
$bus->onConnection('analytics')->publish($message);
```
