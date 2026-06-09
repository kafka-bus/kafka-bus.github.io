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

## Опциональные интерфейсы сообщений

Реализуйте любую комбинацию этих интерфейсов в классе сообщения, чтобы управлять способом доставки.

### HasKey

Возвращает ключ партиции Kafka для сообщения. Сообщения с одинаковым ключом всегда попадают в одну партицию, что гарантирует упорядоченность для этого ключа.

```php
use KafkaBus\Core\Interfaces\Producers\Messages\HasKey;

public function getKey(): ?string
{
    return (string) $this->orderId; // все события одного заказа попадают в одну партицию
}
```

Верните `null`, чтобы Kafka сама выбрала партицию (round-robin или случайным образом, зависит от версии брокера).

### HasHeaders

Прикрепляет метаданные-заголовки к сообщению. Заголовки объединяются с теми, что добавлены через `ProducerPipelineHandler::withHeader()`; при дублировании ключей приоритет имеют заголовки, переданные через `withHeader()`.

```php
use KafkaBus\Core\Interfaces\Producers\Messages\HasHeaders;

public function getHeaders(): array
{
    return [
        'event'   => 'order.created',
        'version' => '2',
        'source'  => 'order-service',
    ];
}
```

### HasPartition

Закрепляет сообщение за конкретным номером партиции. Значение ограничено снизу константой `RD_KAFKA_PARTITION_UA` (`-1`), поэтому возврат отрицательного числа равнозначен отсутствию реализации интерфейса.

```php
use KafkaBus\Core\Interfaces\Producers\Messages\HasPartition;

public function getPartition(): int
{
    return 3; // всегда доставлять в партицию 3
}
```

> Предпочитайте `HasKey` для гарантии упорядоченности. `HasPartition` используйте только тогда, когда нужен явный контроль над партицией — например, при потреблении и повторной публикации в зеркальный топик.

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
