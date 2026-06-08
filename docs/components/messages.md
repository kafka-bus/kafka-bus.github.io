# kafka-bus-messages

Пакет для структурирования, сериализации и десериализации Kafka-сообщений. Предоставляет типизированные `Payload` с автоматическим кастингом полей, доменные сообщения с поддержкой событий и тест-фабрики.

## Установка

```bash
composer require kafka-bus/messages
```

## Ключевые концепции

| Класс           | Описание                                                               |
|-----------------|------------------------------------------------------------------------|
| `Payload`       | Гибкий key-value контейнер с типизированным кастингом                  |
| `JsonMessage`   | `Payload`, который сериализуется напрямую в JSON-сообщение для Kafka   |
| `DomainMessage` | Структурированное сообщение с типом события и списком изменённых полей |
| **Casters**     | Классы для преобразования значений при чтении и записи                 |

## Payload

Базовый класс для типизированных данных сообщения. Расширьте его и объявите кастеры для автоматического приведения типов:

```php
use KafkaBus\Messages\Data\Payload;
use KafkaBus\Messages\Data\Casters\IntegerCaster;
use KafkaBus\Messages\Data\Casters\PayloadCaster;
use KafkaBus\Messages\Data\Casters\CollectionCaster;

/**
 * @property int             $id
 * @property string          $name
 * @property CategoryPayload $category
 * @property AttributePayload[] $attributes
 */
class ProductPayload extends Payload
{
    protected function definitionCasters(): array
    {
        return [
            'id'         => new IntegerCaster(),
            'category'   => new PayloadCaster(CategoryPayload::class),
            'attributes' => new CollectionCaster(new PayloadCaster(AttributePayload::class)),
        ];
    }
}

$product = ProductPayload::from([
    'id'   => '42',              // строка → int
    'name' => 'Laptop',
    'category'   => ['id' => 1, 'name' => 'Electronics'],
    'attributes' => [
        ['id' => 10, 'name' => 'Color', 'value' => 'Silver'],
    ],
]);

echo $product->id;                    // int(42)
echo $product->category->name;        // string("Electronics")
echo $product->attributes[0]->value;  // string("Silver")
```

## Доступные кастеры

| Кастер             | Описание                                                     |
|--------------------|--------------------------------------------------------------|
| `IntegerCaster`    | Приводит к `int`                                             |
| `FloatCaster`      | Приводит к `float`                                           |
| `DateTimeCaster`   | Парсит/форматирует `DateTimeInterface`, формат настраивается |
| `PayloadCaster`    | Гидрирует вложенный `Payload`-подкласс из массива            |
| `CollectionCaster` | Применяет другой кастер к каждому элементу массива           |
| `NullableCaster`   | Оборачивает любой кастер, разрешая `null`                    |

```php
use KafkaBus\Messages\Data\Casters\DateTimeCaster;
use KafkaBus\Messages\Data\Casters\NullableCaster;
use KafkaBus\Messages\Data\Casters\FloatCaster;

protected function definitionCasters(): array
{
    return [
        'published_at' => new DateTimeCaster('Y-m-d\TH:i:s.uP'),
        'deleted_at'   => new NullableCaster(new DateTimeCaster()),
        'price'        => new FloatCaster(),
    ];
}
```

## JsonMessage

`JsonMessage` расширяет `Payload` и реализует `ProducerMessageInterface` — его можно публиковать напрямую в Kafka:

```php
use KafkaBus\Messages\JsonMessage;

$message = new JsonMessage([
    'order_id' => 123,
    'status'   => 'shipped',
    'items'    => [1, 2, 3],
]);

// Payload → {"order_id":123,"status":"shipped","items":[1,2,3]}
$bus->publish($message);
```

## DomainMessage

Структурированное сообщение для event-driven архитектур. Оборачивает объект атрибутов, тип доменного события (`create`/`update`/`delete`) и список изменённых полей (`dirty`).

### Создание класса сообщения

```php
use Micromus\KafkaBusMessages\DomainMessage;

/**
 * @property int    $id
 * @property string $name
 * @property float  $price
 */
class ProductMessage extends DomainMessage
{
    public function getKey(): ?string
    {
        // Kafka partition key — все события одного продукта в одну партицию
        return (string) $this->id;
    }

    protected function definitionCasters(): array
    {
        return [
            'id'    => new IntegerCaster(),
            'price' => new FloatCaster(),
        ];
    }
}
```

### Produce

```php
use KafkaBus\Messages\DomainEventEnum;

$message = new ProductMessage(
    attributes: ['id' => 42, 'name' => 'Laptop Pro', 'price' => 1299.99],
    event: DomainEventEnum::Update,
    dirty: ['name', 'price'],
);

$bus->publish($message);
```

Kafka-сообщение (payload):

```json
{
  "event": "update",
  "attributes": {
    "id": 42,
    "name": "Laptop Pro",
    "price": 1299.99
  },
  "dirty": ["name", "price"]
}
```

### Consume

Используйте атрибут `#[MessageFactory]` с `DomainMessageFactory`:

```php
use KafkaBus\Core\Consumers\Attributes\MessageFactory;
use KafkaBus\Messages\Factories\DomainMessageFactory;

class ProductConsumer
{
    #[MessageFactory(new DomainMessageFactory(ProductMessage::class))]
    public function __invoke(ProductMessage $message): void
    {
        echo $message->getEvent()->value; // 'create' | 'update' | 'delete'
        echo $message->id;           // int(42) — после кастинга
        echo $message->name;         // string("Laptop Pro")

        // Список изменённых полей
        if (in_array('price', $message->getDirty())) {
            $this->updatePriceIndex($message->id, $message->price);
        }
    }
}
```

## Тест-фабрики

Пакет включает базовые фабрики для генерации реалистичных тестовых данных через [FakerPHP](https://github.com/FakerPHP/Faker).

### DomainMessageTestFactory

```php
use KafkaBus\Messages\Testing\DomainMessageTestFactory;

/**
 * @extends DomainMessageTestFactory<ProductPayload>
 */
final class ProductTestFactory extends DomainMessageTestFactory
{
    protected string $messageClass = ProductMessage::class;

    public function definition(): array
    {
        return [
            'id'    => $this->faker->numberBetween(1, 9999),
            'name'  => $this->faker->sentence(),
            'price' => $this->faker->randomFloat(2, 10, 9999),
        ];
    }
}
```

Использование:

```php
// Типизированный DomainMessage с дефолтными fake-данными
$message = ProductTestFactory::new()->message();

// Переопределение полей и события
$message = ProductTestFactory::new()
    ->withEvent(DomainEventEnum::Delete)
    ->withDirty(['name', 'price'])
    ->message(['name' => 'Custom Name']);

// RdKafka\Message для low-level тестов consumer'а
$rdKafkaMessage = ProductTestFactory::new()->make();

// Чистый массив атрибутов
$array = ProductTestFactory::new()->makeArray();
```

### PayloadTestFactory

```php
use KafkaBus\Messages\Testing\PayloadTestFactory;

/**
 * @extends PayloadTestFactory<CategoryPayload>
 */
final class CategoryTestFactory extends PayloadTestFactory
{
    protected string $payloadClass = CategoryPayload::class;

    public function definition(): array
    {
        return [
            'id'   => $this->faker->numberBetween(1, 100),
            'name' => $this->faker->word(),
        ];
    }
}

$category = CategoryTestFactory::new()->payload();
$array    = CategoryTestFactory::new()->makeArray();
```
