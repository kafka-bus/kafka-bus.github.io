# Producer (Laravel)

## Создание класса сообщения

Создайте класс в `app/Kafka/Messages/` и зарегистрируйте его в конфиге:

```php
// app/Kafka/Messages/ProductMessage.php

namespace App\Kafka\Messages;

use Micromus\KafkaBus\Interfaces\Producers\Messages\ProducerMessageInterface;

final readonly class ProductMessage implements ProducerMessageInterface
{
    public function __construct(
        private int    $id,
        private string $name,
        private string $status,
    ) {}

    public function toPayload(): string
    {
        return json_encode([
            'id'     => $this->id,
            'name'   => $this->name,
            'status' => $this->status,
        ]);
    }

    public function getKey(): ?string
    {
        // Kafka partition key — все события одного продукта попадут в одну партицию
        return (string) $this->id;
    }

    public function getHeaders(): array
    {
        return ['event' => 'product.updated'];
    }
}
```

Зарегистрируйте в конфиге:

```php
// config/kafka-bus.php
'producers' => [
    'routes' => [
        App\Kafka\Messages\ProductMessage::class => 'products',
    ],
],
```

## Публикация через Facade

```php
use Micromus\KafkaBusLaravel\Facades\KafkaBus;

KafkaBus::publish(new ProductMessage(
    id:     $product->id,
    name:   $product->name,
    status: $product->status,
));
```

## Публикация через Dependency Injection

Рекомендуется для Action/Service-классов — упрощает тестирование:

```php
use Micromus\KafkaBus\Interfaces\Bus\BusInterface;

class UpdateProductAction
{
    public function __construct(
        private readonly BusInterface $bus,
    ) {}

    public function execute(Product $product): void
    {
        $product->save();

        $this->bus->publish(new ProductMessage(
            id:     $product->id,
            name:   $product->name,
            status: $product->status,
        ));
    }
}
```

## Публикация через другое соединение

```php
KafkaBus::onConnection('analytics')->publish($message);

// Или через DI
$this->bus->onConnection('analytics')->publish($message);
```

## Интеграция с kafka-bus-messages

Если установлен пакет `kafka-bus-messages`, используйте `DomainMessage` для структурированных событий:

```php
use Micromus\KafkaBusMessages\DomainMessage;
use Micromus\KafkaBusMessages\DomainEventEnum;

// В обработчике события
$message = new ProductMessage(
    attributes: $product->toArray(),
    event: DomainEventEnum::Update,
    dirty: $product->getDirty(),
);

KafkaBus::publish($message);
```

Payload будет выглядеть так:

```json
{
  "event": "update",
  "attributes": { "id": 42, "name": "Laptop Pro", "status": "active" },
  "dirty": ["name"]
}
```

Подробнее о сообщениях — в разделе [Messages](/packages/messages).

## Добавление idempotency key

Для гарантированной однократной обработки на стороне consumer'а реализуйте `HasIdempotency`:

```php
use Micromus\KafkaBusCommiter\Interfaces\HasIdempotency;

final readonly class ProductMessage implements ProducerMessageInterface, HasIdempotency
{
    public function getIdempotencyKey(): string
    {
        return "product-{$this->id}-v{$this->version}";
    }
}
```

И включите middleware в конфиге:

```php
'producers' => [
    'middleware' => [
        \Micromus\KafkaBusCommiter\Middleware\ProducerIdempotencyMiddleware::class,
    ],
],
```

Подробнее — в разделе [Commiter](/packages/commiter).
