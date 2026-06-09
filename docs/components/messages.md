# kafka-bus-messages

A package for structuring, serializing, and deserializing Kafka messages. Provides typed `Payload` with automatic field casting, domain messages with event support, and test factories.

## Installation

```bash
composer require kafka-bus/messages
```

## Key Concepts

| Class           | Description                                                                    |
|-----------------|--------------------------------------------------------------------------------|
| `Payload`       | Flexible key-value container with typed casting                                |
| `JsonMessage`   | A `Payload` that serializes directly into a JSON Kafka message                 |
| `DomainMessage` | Structured message with event type and a list of changed fields                |
| **Casters**     | Classes for transforming values on read and write                              |

## Payload

Base class for typed message data. Extend it and declare casters for automatic type coercion:

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
    'id'   => '42',              // string → int
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

## Available Casters

| Caster             | Description                                                         |
|--------------------|---------------------------------------------------------------------|
| `IntegerCaster`    | Casts to `int`                                                      |
| `FloatCaster`      | Casts to `float`                                                    |
| `DateTimeCaster`   | Parses/formats `DateTimeInterface`, format is configurable          |
| `PayloadCaster`    | Hydrates a nested `Payload` subclass from an array                  |
| `CollectionCaster` | Applies another caster to each element of an array                  |
| `NullableCaster`   | Wraps any caster, allowing `null`                                   |

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

`JsonMessage` extends `Payload` and implements `ProducerMessageInterface` — it can be published directly to Kafka:

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

A structured message for event-driven architectures. Wraps an attributes object, a domain event type (`create`/`update`/`delete`), and a list of changed fields (`dirty`).

### Creating a Message Class

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
        // Kafka partition key — all events for the same product go to the same partition
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

Kafka message (payload):

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

Use the `#[MessageFactory]` attribute with `DomainMessageFactory`:

```php
use KafkaBus\Core\Consumers\Attributes\MessageFactory;
use KafkaBus\Messages\Factories\DomainMessageFactory;

class ProductConsumer
{
    #[MessageFactory(new DomainMessageFactory(ProductMessage::class))]
    public function __invoke(ProductMessage $message): void
    {
        echo $message->getEvent()->value; // 'create' | 'update' | 'delete'
        echo $message->id;           // int(42) — after casting
        echo $message->name;         // string("Laptop Pro")

        // List of changed fields
        if (in_array('price', $message->getDirty())) {
            $this->updatePriceIndex($message->id, $message->price);
        }
    }
}
```

## Test Factories

The package includes base factories for generating realistic test data via [FakerPHP](https://github.com/FakerPHP/Faker).

### DomainMessageTestFactory

```php
use KafkaBus\Messages\Testing\DomainMessageTestFactory;

/**
 * @extends DomainMessageTestFactory<ProductPayload>
 */
final class ProductTestFactory extends DomainMessageTestFactory
{
    protected string $messageClass = ProductMessage::class;
    
    protected string $topicKey = 'products';

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

Usage:

```php
// Typed DomainMessage with default fake data
$message = ProductTestFactory::new()->message();

// Override fields and event
$message = ProductTestFactory::new()
    ->withEvent(DomainEventEnum::Delete)
    ->withDirty(['name', 'price'])
    ->message(['name' => 'Custom Name']);

// RdKafka\Message for low-level consumer tests
$rdKafkaMessage = ProductTestFactory::new()->make();

// Plain attributes array
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
