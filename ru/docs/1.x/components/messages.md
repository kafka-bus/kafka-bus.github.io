# kafka-bus/messages

Пакет для структурирования, сериализации и десериализации сообщений Kafka. Предоставляет типизированный `Payload` с автоматическим кастингом полей, доменные сообщения с поддержкой событий и фабрики для тестов.

## Установка

```bash
composer require kafka-bus/messages
```

Зависимости: `php ^8.2`, `ext-rdkafka`, `kafka-bus/core`, `webmozart/assert`.

## Ключевые концепции

| Класс           | Описание                                                                |
|-----------------|-------------------------------------------------------------------------|
| `Payload`       | Гибкий контейнер ключ-значение с типизированным кастингом               |
| `JsonMessage`   | `Payload`, который сериализуется напрямую в JSON-сообщение Kafka        |
| `DomainMessage` | Структурированное сообщение с типом события и списком изменённых полей  |
| **Casters**     | Классы для преобразования значений при чтении и записи                  |
| **Factories**   | `DomainMessageFactory`, `JsonMessageFactory` — десериализация из Kafka  |

## Payload

Базовый класс для типизированных данных сообщения. Расширьте его и объявите кастеры для автоматического приведения типов:

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
    'name' => 'Ноутбук',
    'category'   => ['id' => 1, 'name' => 'Электроника'],
    'attributes' => [
        ['id' => 10, 'name' => 'Цвет', 'value' => 'Серебристый'],
    ],
]);

echo $product->id;                    // int(42)
echo $product->category->name;        // string("Электроника")
echo $product->attributes[0]->value;  // string("Серебристый")
```

### Как работает кастинг

Кастинг двусторонний:

- **`cast()`** вызывается при записи значения — в конструкторе, через `$payload->field = ...` или `$payload['field'] = ...`;
- **`rollback()`** вызывается при сериализации в `jsonSerialize()`.

Поля без объявленного кастера сохраняются и отдаются как есть.

::: tip
Кастинг срабатывает на **каждое** присваивание, а не только при создании. `$product->id = '100'` тоже пройдёт через `IntegerCaster`.
:::

### Доступ к данным

`Payload` реализует `ArrayAccess` и магические методы, поэтому доступен четырьмя способами:

```php
$product->name;             // магический __get
$product['name'];           // ArrayAccess
isset($product->name);      // __isset / offsetExists
unset($product->name);      // __unset / offsetUnset

$product->name = 'Новое';   // __set → cast()
$product['name'] = 'Новое'; // offsetSet → cast()
```

::: warning
Обращение к несуществующему полю возвращает `null`, а не бросает исключение. Опечатка в имени поля молча даст `null` — статический анализ через `@property`-аннотации помогает это ловить.
:::

### Сериализация

`jsonSerialize()` прогоняет все атрибуты через `rollback()` соответствующих кастеров и возвращает массив, готовый к `json_encode()`. Порядок ключей соответствует порядку присваивания.

```php
$product->jsonSerialize();
// ['id' => 42, 'name' => 'Ноутбук', 'category' => ['id' => 1, ...], ...]
```

## Доступные кастеры

| Кастер             | `cast()`                                    | `rollback()`                              |
|--------------------|---------------------------------------------|-------------------------------------------|
| `IntegerCaster`    | `intval()`                                  | `intval()`                                |
| `FloatCaster`      | `floatval()`                                | `floatval()`                              |
| `DateTimeCaster`   | Строка → `DateTimeImmutable` по формату     | Форматирует, предварительно переведя в UTC |
| `EnumCaster`       | `string\|int` → `BackedEnum::tryFrom()`     | Возвращает `->value`                      |
| `PayloadCaster`    | `array` → вложенный `Payload`               | `jsonSerialize()` вложенного payload      |
| `CollectionCaster` | Применяет вложенный кастер к каждому элементу | То же для `rollback()`                  |
| `NullableCaster`   | Пропускает `null`, иначе делегирует         | Пропускает `null`, иначе делегирует       |

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

Формат по умолчанию — `Y-m-d\TH:i:s.uP` (ISO 8601 с микросекундами и смещением). Значение, уже являющееся `DateTimeInterface`, пропускается без изменений. Если строку не удалось разобрать по формату, бросается `LogicException`.

::: warning
При сериализации дата принудительно переводится в **UTC**. Если вы записали `2024-05-01 12:00:00 +03:00`, в Kafka уйдёт `2024-05-01T09:00:00.000000+00:00`. Это делает представление стабильным между сервисами, но означает, что исходная таймзона теряется.
:::

### EnumCaster

Работает только с `BackedEnum`. Класс проверяется в конструкторе через `webmozart/assert` — ошибка обнаружится сразу при создании кастера.

```php
new EnumCaster(StatusEnum::class);                          // неизвестное значение → null
new EnumCaster(StatusEnum::class, default: StatusEnum::New); // неизвестное значение → New
```

Значение неверного типа (не `string` и не `int`) вызывает `InvalidArgumentException`.

### NullableCaster

Любой кастер, кроме `CollectionCaster`, падает на `null`. Оборачивайте необязательные поля:

```php
'deleted_at' => new NullableCaster(new DateTimeCaster()),
'rating'     => new NullableCaster(new FloatCaster()),
```

`CollectionCaster` обрабатывает `null` самостоятельно, превращая его в пустой массив.

### Собственный кастер

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

Аргумент `$attributeKey` содержит имя поля (для вложенных коллекций — путь вида `attributes.0`) и используется в сообщениях об ошибках.

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

Для типизации создайте подкласс с кастерами — как у обычного `Payload`.

### Получение сообщения

```php
use KafkaBus\Core\Consumers\Attributes\MessageFactory;
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
Не путайте две одноимённые фабрики: `KafkaBus\Messages\Factories\JsonMessageFactory` гидратирует ваш подкласс `JsonMessage` и требует его класс в конструкторе, а `KafkaBus\Core\Consumers\Messages\JsonMessageFactory` просто возвращает `array` из `json_decode()`.
:::

## DomainMessage

Структурированное сообщение для event-driven архитектур. Оборачивает атрибуты, тип доменного события (`create`/`update`/`delete`) и список изменённых полей (`dirty`).

### Создание класса сообщения

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
        // Ключ партиции Kafka — все события одного продукта идут в одну партицию
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

::: warning `getKey()` обязателен
`DomainMessage` объявлен как `abstract` и реализует `HasKey`, но **не** предоставляет `getKey()`. Каждый подкласс обязан его определить, иначе класс не скомпилируется.
:::

### Публикация

```php
use KafkaBus\Messages\DomainEventEnum;

$message = new ProductMessage(
    attributes: ['id' => 42, 'name' => 'Ноутбук Pro', 'price' => 1299.99],
    event: DomainEventEnum::Update,
    dirty: ['name', 'price'],
);

// Эквивалентно:
$message = ProductMessage::create(
    ['id' => 42, 'name' => 'Ноутбук Pro', 'price' => 1299.99],
    DomainEventEnum::Update,
    ['name', 'price'],
);

$bus->publish($message);
```

Payload сообщения в Kafka:

```json
{
  "event": "update",
  "attributes": {
    "id": 42,
    "name": "Ноутбук Pro",
    "price": 1299.99
  },
  "dirty": ["name", "price"]
}
```

`DomainEventEnum` — backed enum со значениями `create`, `update`, `delete`; по умолчанию используется `Create`.

### Получение

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
        echo $message->id;                // int(42) — после кастинга
        echo $message->name;              // string("Ноутбук Pro")

        if (in_array('price', $message->getDirty(), true)) {
            $this->updatePriceIndex($message->id, $message->price);
        }
    }
}
```

::: warning Методы, а не свойства
`event` и `dirty` — приватные поля с геттерами `getEvent()` и `getDirty()`. Обращение `$message->event` пойдёт в `Payload::__get()` и вернёт `null`, потому что такого атрибута в payload нет.
:::

`DomainMessageFactory` устойчива к неполным сообщениям: отсутствующий или неизвестный `event` превращается в `DomainEventEnum::Create`, отсутствующие `attributes` и `dirty` — в пустые массивы.

### Обработка по типу события

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

`dirty` позволяет обрабатывать только реально изменившиеся поля — это особенно полезно для дорогих операций вроде переиндексации.

## Фабрики для тестов

Пакет включает базовые фабрики для генерации реалистичных тестовых данных через [FakerPHP](https://github.com/FakerPHP/Faker). Генератор создаётся с локалью `ru_RU`.

Все фабрики иммутабельны: каждый `with*` возвращает клон.

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

Использование:

```php
// Типизированный DomainMessage с дефолтными fake-данными
$message = ProductTestFactory::new()->message();

// Переопределение полей и события
$message = ProductTestFactory::new()
    ->withEvent(DomainEventEnum::Delete)
    ->withDirty(['name', 'price'])
    ->message(['name' => 'Кастомное имя']);

// RdKafka\Message для низкоуровневых тестов консьюмера
$rdKafkaMessage = ProductTestFactory::new()
    ->withHeaders(['x-idempotency-key' => 'prod-42'])
    ->withPartition(1)
    ->withOffset(10)
    ->make();

// Обычный массив — уже в формате {event, attributes, dirty}
$array = ProductTestFactory::new()->makeArray();
```

### Методы фабрик

| Метод                            | Описание                                                        |
|----------------------------------|-----------------------------------------------------------------|
| `definition()`                   | Абстрактный: базовый набор атрибутов                            |
| `withState(array $state)`        | Переопределяет часть атрибутов                                  |
| `withEvent(DomainEventEnum)`     | Тип события (только `DomainMessageTestFactory`)                 |
| `withDirty(array $dirty)`        | Список изменённых полей (только `DomainMessageTestFactory`)     |
| `withHeaders(array $headers)`    | Заголовки итогового `RdKafka\Message`                           |
| `withKey(?string $key)`          | Ключ партиционирования                                          |
| `withTopicKey(?string $topicKey)`| Топик сообщения                                                 |
| `withPartition(int $partition)`  | Номер партиции                                                  |
| `withOffset(int $offset)`        | Offset                                                          |
| `makeArray(array $extra = [])`   | Массив атрибутов                                                |
| `make(array $extra = [])`        | `RdKafka\Message`                                               |
| `message(array $extra = [])`     | Готовый `DomainMessage` (только `DomainMessageTestFactory`)     |

::: tip Приоритет атрибутов
Значения объединяются в порядке `definition()` → `withState()` → аргумент `$extra`. Последний выигрывает.
:::

### PayloadTestFactory

Для payload без обвязки `DomainMessage`:

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

$payload = ProductPayloadFactory::new()->payload(['name' => 'Ноутбук']);
```

В отличие от `DomainMessageTestFactory`, здесь `makeArray()` возвращает «плоский» массив атрибутов без обёртки `{event, attributes, dirty}`.
