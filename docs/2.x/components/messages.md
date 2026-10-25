---
title: Messages
description: Typed payloads with automatic casting, domain event messages, and test factories
---

# kafka-bus/messages

A package for structuring, serializing, and deserializing Kafka messages. Provides a typed `Payload` with automatic field casting, domain messages with event support, and test factories.

## Installation

```bash
composer require kafka-bus/messages
```

Dependencies: `php ^8.2`, `ext-rdkafka`, `kafka-bus/core`, `webmozart/assert`.

## Key Concepts

| Class            | Description                                                              |
|-------------------|------------------------------------------------------------------------------|
| `Payload`        | A flexible key-value container with typed casting                            |
| `JsonMessage`    | A `Payload` that serializes directly into a JSON Kafka message               |
| `DomainMessage`  | A structured message with an event type and a list of changed fields         |
| **Casters**      | Classes that convert values on read and write                                |
| **Factories**    | `DomainMessageFactory`, `JsonMessageFactory` — deserialization from Kafka     |

## Payload

The base class for typed message data. Extend it and declare casters for automatic type coercion:

```php
use KafkaBus\Messages\Data\Casters\CollectionCaster;
use KafkaBus\Messages\Data\Casters\IntegerCaster;
use KafkaBus\Messages\Data\Casters\PayloadCaster;
use KafkaBus\Messages\Data\Payload;

/**
 * @property int                $id
 * @property string             $name
 * @property CategoryPayload    $category
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

### How Casting Works

Casting is two-way:

- **`cast()`** runs when a value is written — in the constructor, via `$payload->field = ...`, or `$payload['field'] = ...`;
- **`rollback()`** runs during serialization in `jsonSerialize()`.

Fields with no declared caster are stored and returned as-is.

::: tip
Casting runs on **every** assignment, not just on creation. `$product->id = '100'` also goes through `IntegerCaster`.
:::

### Accessing Data

`Payload` implements `ArrayAccess` and magic methods, so it's accessible in four ways:

```php
$product->name;             // magic __get
$product['name'];           // ArrayAccess
isset($product->name);      // __isset / offsetExists
unset($product->name);      // __unset / offsetUnset

$product->name = 'New';     // __set → cast()
$product['name'] = 'New';   // offsetSet → cast()
```

::: warning
Accessing a nonexistent field returns `null` instead of throwing. A typo in a field name silently produces `null` — static analysis via `@property` annotations helps catch this.
:::

### Serialization

`jsonSerialize()` runs every attribute through its caster's `rollback()` and returns an array ready for `json_encode()`. Key order matches assignment order.

```php
$product->jsonSerialize();
// ['id' => 42, 'name' => 'Laptop', 'category' => ['id' => 1, ...], ...]
```

## Available Casters

| Caster              | `cast()`                                     | `rollback()`                                |
|-----------------------|-------------------------------------------------|--------------------------------------------------|
| `IntegerCaster`    | `intval()`                                    | `intval()`                                      |
| `FloatCaster`      | `floatval()`                                  | `floatval()`                                    |
| `DateTimeCaster`   | String → `DateTimeImmutable` per format        | Formats after converting to UTC                  |
| `EnumCaster`       | `string\|int` → `BackedEnum::tryFrom()`       | Returns `->value`                                |
| `PayloadCaster`    | `array` → nested `Payload`                    | Nested payload's `jsonSerialize()`               |
| `CollectionCaster` | Applies the inner caster to each element       | Same for `rollback()`                            |
| `NullableCaster`   | Passes `null` through, otherwise delegates     | Passes `null` through, otherwise delegates       |

```php
use KafkaBus\Messages\Data\Casters\DateTimeCaster;
use KafkaBus\Messages\Data\Casters\EnumCaster;
use KafkaBus\Messages\Data\Casters\FloatCaster;
use KafkaBus\Messages\Data\Casters\NullableCaster;

protected function definitionCasters(): array
{
    return [
        'published_at' => new DateTimeCaster('Y-m-d\TH:i:s.uP'),
        'deleted_at'   => new NullableCaster(new DateTimeCaster()),
        'price'        => new FloatCaster(),
        'status'       => new EnumCaster(StatusEnum::class, default: StatusEnum::Draft),
    ];
}
```

### DateTimeCaster

The default format is `Y-m-d\TH:i:s.uP` (ISO 8601 with microseconds and offset). A value that's already a `DateTimeInterface` passes through unchanged. If a string can't be parsed against the format, a `LogicException` is thrown.

::: warning
On serialization, the date is forced into **UTC**. If you stored `2024-05-01 12:00:00 +03:00`, Kafka receives `2024-05-01T09:00:00.000000+00:00`. This keeps the representation stable across services, but the original timezone is lost.
:::

### EnumCaster

Only works with `BackedEnum`. The class is validated in the constructor via `webmozart/assert` — a mistake is caught immediately when the caster is created.

```php
new EnumCaster(StatusEnum::class);                          // unknown value → null
new EnumCaster(StatusEnum::class, default: StatusEnum::New); // unknown value → New
```

A value of the wrong type (not `string` or `int`) throws an `InvalidArgumentException`.

### NullableCaster

Every caster except `CollectionCaster` fails on `null`. Wrap optional fields with it:

```php
'deleted_at' => new NullableCaster(new DateTimeCaster()),
'rating'     => new NullableCaster(new FloatCaster()),
```

`CollectionCaster` handles `null` on its own, turning it into an empty array.

### Custom Caster

```php
use KafkaBus\Messages\Interfaces\CasterInterface;

final class MoneyCaster implements CasterInterface
{
    public function cast(mixed $value, string $attributeKey): Money
    {
        return Money::fromMinorUnits((int) $value);
    }

    public function rollback(mixed $value, string $attributeKey): int
    {
        return $value instanceof Money ? $value->minorUnits() : (int) $value;
    }
}
```

The `$attributeKey` argument holds the field name (for nested collections, a path like `attributes.0`) and is used in error messages.

## JsonMessage

`JsonMessage` extends `Payload` and implements `ProducerMessageInterface` — you can publish it directly to Kafka:

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

For typing, create a subclass with casters — just like a regular `Payload`.

### Retrieving a Message

```php
use KafkaBus\Core\Receivers\Attributes\MessageFactory;
use KafkaBus\Messages\Factories\JsonMessageFactory;

class OrderHandler
{
    #[MessageFactory(new JsonMessageFactory(OrderJsonMessage::class))]
    public function __invoke(OrderJsonMessage $message): void
    {
        echo $message->order_id;
    }
}
```

::: warning
Don't confuse the two same-named factories: `KafkaBus\Messages\Factories\JsonMessageFactory` hydrates your `JsonMessage` subclass and requires its class in the constructor, while `KafkaBus\Core\Receivers\Messages\JsonMessageFactory` simply returns an `array` from `json_decode()`.
:::

## DomainMessage

A structured message for event-driven architectures. Wraps attributes, a domain event type (`create`/`update`/`delete`), and a list of changed fields (`dirty`).

### Creating a Message Class

```php
use KafkaBus\Messages\Data\Casters\FloatCaster;
use KafkaBus\Messages\Data\Casters\IntegerCaster;
use KafkaBus\Messages\DomainMessage;

/**
 * @property int    $id
 * @property string $name
 * @property float  $price
 */
class ProductMessage extends DomainMessage
{
    public function getKey(): ?string
    {
        // Kafka partition key — all events for one product go to the same partition
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

::: warning `getKey()` is required
`DomainMessage` is declared `abstract` and implements `HasKey`, but it doesn't provide `getKey()`. Every subclass must define it, or the class won't compile.
:::

### Publishing

```php
use KafkaBus\Messages\DomainEventEnum;

$message = new ProductMessage(
    attributes: ['id' => 42, 'name' => 'Laptop Pro', 'price' => 1299.99],
    event: DomainEventEnum::Update,
    dirty: ['name', 'price'],
);

// Equivalent to:
$message = ProductMessage::create(
    ['id' => 42, 'name' => 'Laptop Pro', 'price' => 1299.99],
    DomainEventEnum::Update,
    ['name', 'price'],
);

$bus->publish($message);
```

The message payload in Kafka:

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

`DomainEventEnum` is a backed enum with values `create`, `update`, `delete`; `Create` is the default.

### Receiving

Use the `#[MessageFactory]` attribute with `DomainMessageFactory`:

```php
use KafkaBus\Core\Receivers\Attributes\MessageFactory;
use KafkaBus\Messages\Factories\DomainMessageFactory;

class ProductConsumer
{
    #[MessageFactory(new DomainMessageFactory(ProductMessage::class))]
    public function __invoke(ProductMessage $message): void
    {
        echo $message->getEvent()->value; // 'create' | 'update' | 'delete'
        echo $message->id;                // int(42) — after casting
        echo $message->name;              // string("Laptop Pro")

        if (in_array('price', $message->getDirty(), true)) {
            $this->updatePriceIndex($message->id, $message->price);
        }
    }
}
```

::: warning Methods, not properties
`event` and `dirty` are private fields with `getEvent()` and `getDirty()` getters. Accessing `$message->event` goes through `Payload::__get()` and returns `null`, because there's no such attribute in the payload.
:::

`DomainMessageFactory` tolerates incomplete messages: a missing or unknown `event` becomes `DomainEventEnum::Create`, and missing `attributes` or `dirty` become empty arrays.

### Handling by Event Type

```php
class ProductConsumer
{
    #[MessageFactory(new DomainMessageFactory(ProductMessage::class))]
    public function __invoke(ProductMessage $message): void
    {
        match ($message->getEvent()) {
            DomainEventEnum::Create => $this->onCreate($message),
            DomainEventEnum::Update => $this->onUpdate($message),
            DomainEventEnum::Delete => $this->onDelete($message),
        };
    }
}
```

`dirty` lets you process only the fields that actually changed — especially useful for expensive operations like reindexing.

## Test Factories

The package includes base factories for generating realistic test data via [FakerPHP](https://github.com/FakerPHP/Faker). The generator is created with the `ru_RU` locale.

All factories are immutable: every `with*` call returns a clone.

### DomainMessageTestFactory

```php
use KafkaBus\Messages\Testing\DomainMessageTestFactory;

/**
 * @extends DomainMessageTestFactory<ProductMessage>
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
// A typed DomainMessage with default fake data
$message = ProductTestFactory::new()->message();

// Overriding fields and the event
$message = ProductTestFactory::new()
    ->withEvent(DomainEventEnum::Delete)
    ->withDirty(['name', 'price'])
    ->message(['name' => 'Custom Name']);

// RdKafka\Message for low-level consumer tests
$rdKafkaMessage = ProductTestFactory::new()
    ->withHeaders(['x-idempotency-key' => 'prod-42'])
    ->withPartition(1)
    ->withOffset(10)
    ->make();

// A plain array — already in the {event, attributes, dirty} format
$array = ProductTestFactory::new()->makeArray();
```

### Factory Methods

| Method                             | Description                                                        |
|---------------------------------------|--------------------------------------------------------------------------|
| `definition()`                    | Abstract: the base set of attributes                                       |
| `withState(array $state)`         | Overrides part of the attributes                                           |
| `withEvent(DomainEventEnum)`     | The event type (`DomainMessageTestFactory` only)                            |
| `withDirty(array $dirty)`        | The list of changed fields (`DomainMessageTestFactory` only)               |
| `withHeaders(array $headers)`    | Headers for the resulting `RdKafka\Message`                                 |
| `withKey(?string $key)`          | The partition key                                                           |
| `withTopicKey(?string $topicKey)`| The message's topic                                                         |
| `withPartition(int $partition)`  | The partition number                                                        |
| `withOffset(int $offset)`        | The offset                                                                   |
| `makeArray(array $extra = [])`   | An array of attributes                                                      |
| `make(array $extra = [])`        | An `RdKafka\Message`                                                        |
| `message(array $extra = [])`     | The finished `DomainMessage` (`DomainMessageTestFactory` only)             |

::: tip Attribute Priority
Values are merged in the order `definition()` → `withState()` → the `$extra` argument. The last one wins.
:::

### PayloadTestFactory

For a payload with no `DomainMessage` wrapping:

```php
use KafkaBus\Messages\Testing\PayloadTestFactory;

/**
 * @extends PayloadTestFactory<ProductPayload>
 */
final class ProductPayloadFactory extends PayloadTestFactory
{
    protected string $payloadClass = ProductPayload::class;

    public function definition(): array
    {
        return [
            'id'   => $this->faker->numberBetween(1, 9999),
            'name' => $this->faker->sentence(),
        ];
    }
}

$payload = ProductPayloadFactory::new()->payload(['name' => 'Laptop']);
```

Unlike `DomainMessageTestFactory`, here `makeArray()` returns a "flat" array of attributes with no `{event, attributes, dirty}` wrapper.
