# Отправка сообщений

## Создание класса сообщения

Создайте класс в `app/Kafka/Messages/` и зарегистрируйте его в конфиге:

```php
// app/Kafka/Messages/ProductMessage.php

namespace App\Kafka\Messages;

use KafkaBus\Core\Interfaces\Producers\Messages\HasHeaders;
use KafkaBus\Core\Interfaces\Producers\Messages\HasKey;
use KafkaBus\Core\Interfaces\Producers\Messages\ProducerMessageInterface;

final readonly class ProductMessage implements ProducerMessageInterface, HasHeaders, HasKey
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

    // Реализуется при использовании интерфейса HasKey
    public function getKey(): ?string
    {
        // Ключ партиции Kafka — все события одного продукта идут в одну партицию
        return (string) $this->id;
    }

    // Реализуется при использовании интерфейса HasHeaders
    public function getHeaders(): array
    {
        return ['event' => 'product.updated'];
    }
}
```

Регистрация в конфиге:

```php
// config/kafka-bus.php
'producers' => [
    'routes' => [
        App\Kafka\Messages\ProductMessage::class => 'products',
    ],
],
```

Подробнее об интерфейсах `HasKey`, `HasHeaders`, `HasPartition` — в разделе [Отправка сообщений](/ru/docs/producer/producing#опциональные-интерфеисы-сообщении).

## Публикация через фасад

```php
use KafkaBus\Laravel\Facades\KafkaBus;

KafkaBus::publish(new ProductMessage(
    id:     $product->id,
    name:   $product->name,
    status: $product->status,
));
```

## Публикация через внедрение зависимостей

Рекомендуется для Action/Service классов — упрощает тестирование:

```php
use KafkaBus\Core\Interfaces\Bus\BusInterface;

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

`BusInterface` зарегистрирован как singleton, поэтому type-hint резолвится автоматически. `KafkaBus::fake()` подменяет ту же привязку в контейнере — тесты с DI работают так же, как с фасадом.

## Пакетная публикация

```php
use KafkaBus\Core\Bus\MessageBatch;
use KafkaBus\Laravel\Facades\KafkaBus;

$messages = $products
    ->map(fn (Product $p) => new ProductMessage($p->id, $p->name, $p->status))
    ->all();

KafkaBus::publishBatch(MessageBatch::fromArray($messages));
```

::: danger
Все сообщения батча должны быть **одного класса** — маршрут определяется по первому элементу и применяется ко всему батчу. Пустой массив вызовет ошибку. Подробнее — в разделе [Пакетная публикация](/ru/docs/producer/producing#пакетная-публикация).
:::

## Публикация через другое подключение

```php
KafkaBus::onConnection('analytics')->publish($message);

// Или через DI
$this->bus->onConnection('analytics')->publish($message);
```

Маршруты продюсера при этом общие для всех подключений — меняется только целевой брокер.

## Интеграция с kafka-bus/messages

Пакет `kafka-bus/messages` устанавливается вместе с `laravel-bridge`. Используйте `DomainMessage` для структурированных событий:

```php
namespace App\Kafka\Messages;

use KafkaBus\Messages\Data\Casters\IntegerCaster;
use KafkaBus\Messages\DomainMessage;

/**
 * @property int    $id
 * @property string $name
 * @property string $status
 */
class ProductMessage extends DomainMessage
{
    public function getKey(): ?string
    {
        return (string) $this->id;
    }

    protected function definitionCasters(): array
    {
        return ['id' => new IntegerCaster()];
    }
}
```

Публикация из обработчика события модели:

```php
use KafkaBus\Laravel\Facades\KafkaBus;
use KafkaBus\Messages\DomainEventEnum;

$message = new ProductMessage(
    attributes: $product->toArray(),
    event: DomainEventEnum::Update,
    dirty: array_keys($product->getDirty()),
);

KafkaBus::publish($message);
```

Payload будет выглядеть так:

```json
{
  "event": "update",
  "attributes": { "id": 42, "name": "Ноутбук Pro", "status": "active" },
  "dirty": ["name"]
}
```

::: tip
`Model::getDirty()` в Laravel возвращает массив `поле => значение`, а `DomainMessage` ожидает список имён полей. Оборачивайте его в `array_keys()`.
:::

Подробнее о сообщениях — в разделе [Messages](/ru/docs/components/messages).

## Публикация из событий модели

Типичный способ не забыть отправить событие — обсервер:

```php
namespace App\Observers;

use App\Kafka\Messages\ProductMessage;
use KafkaBus\Laravel\Facades\KafkaBus;
use KafkaBus\Messages\DomainEventEnum;

class ProductObserver
{
    public function created(Product $product): void
    {
        KafkaBus::publish(ProductMessage::create($product->toArray(), DomainEventEnum::Create));
    }

    public function updated(Product $product): void
    {
        KafkaBus::publish(ProductMessage::create(
            $product->toArray(),
            DomainEventEnum::Update,
            array_keys($product->getDirty()),
        ));
    }

    public function deleted(Product $product): void
    {
        KafkaBus::publish(ProductMessage::create($product->toArray(), DomainEventEnum::Delete));
    }
}
```

::: warning Публикация и транзакции БД
`publish()` синхронно отправляет сообщение и ждёт `flush()`. Внутри транзакции это опасно: сообщение уйдёт в Kafka, даже если транзакция потом откатится.

Публикуйте после коммита — например, через `DB::afterCommit()` или события `*ed` у моделей вне транзакции. Для строгих гарантий нужен паттерн Outbox.
:::

## Добавление ключа идемпотентности

Для гарантированной однократной обработки на стороне консьюмера реализуйте `HasIdempotency`:

```php
use KafkaBus\Commiter\Interfaces\HasIdempotency;
use KafkaBus\Core\Interfaces\Producers\Messages\ProducerMessageInterface;

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
        KafkaBus\Commiter\Middleware\ProducerIdempotencyMiddleware::class,
    ],
],
```

Middleware добавит заголовок `x-idempotency-key`. Подробнее — в разделе [Commiter](/ru/docs/components/commiter).

## Проверка маршрутов

```bash
php artisan kafka:route:list
```

Команда покажет все зарегистрированные классы сообщений, их топики и middleware.
