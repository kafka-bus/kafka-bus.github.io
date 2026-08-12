# Отправка сообщений

Producer отвечает за отправку сообщений в Kafka. На уровне ядра это `Bus::publish()`, который принимает объект, реализующий `ProducerMessageInterface`.

## Основной интерфейс

Единственный обязательный метод — сериализация в строку:

```php
namespace KafkaBus\Core\Interfaces\Producers\Messages;

interface ProducerMessageInterface
{
    public function toPayload(): string;
}
```

::: warning Не путайте с `ProducerMessage`
`KafkaBus\Core\Producers\Messages\ProducerMessage` — это **результат** работы pipeline, а не то, что вы публикуете. Он собирается внутри `ProducerPipelineHandler` и содержит уже готовые `topic`, `payload`, `headers`, `partition` и `key`. Класс не реализует `ProducerMessageInterface`, поэтому передать его в `publish()` нельзя.
:::

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

::: tip Готовые базовые классы
Если не хочется писать `toPayload()` руками, возьмите `JsonMessage` или `DomainMessage` из пакета [`kafka-bus/messages`](/ru/docs/components/messages) — они уже реализуют `ProducerMessageInterface` поверх типизированного `Payload`.
:::

## Опциональные интерфейсы сообщений

Реализуйте любую комбинацию этих интерфейсов в классе сообщения, чтобы управлять способом доставки. Ни один из них не обязателен — `ProducerPipelineHandler` проверяет их через `instanceof` и подставляет значения по умолчанию.

### HasKey

Возвращает ключ партиции Kafka для сообщения. Сообщения с одинаковым ключом всегда попадают в одну партицию, что гарантирует упорядоченность для этого ключа.

```php
use KafkaBus\Core\Interfaces\Producers\Messages\HasKey;

public function getKey(): ?string
{
    return (string) $this->orderId; // все события одного заказа попадают в одну партицию
}
```

Верните `null`, чтобы Kafka сама выбрала партицию (round-robin или случайным образом, зависит от версии брокера). Без интерфейса ключ всегда `null`.

### HasHeaders

Прикрепляет метаданные-заголовки к сообщению. Заголовки объединяются с теми, что добавлены через `ProducerPipelineHandler::withHeader()`; при дублировании ключей приоритет имеют заголовки, переданные через `withHeader()` из middleware.

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

Закрепляет сообщение за конкретным номером партиции. Значение ограничено снизу константой `RD_KAFKA_PARTITION_UA` (`-1`) через `max()`, поэтому возврат отрицательного числа равнозначен отсутствию реализации интерфейса.

```php
use KafkaBus\Core\Interfaces\Producers\Messages\HasPartition;

public function getPartition(): int
{
    return 3; // всегда доставлять в партицию 3
}
```

> Предпочитайте `HasKey` для гарантии упорядоченности. `HasPartition` используйте только тогда, когда нужен явный контроль над партицией — например, при потреблении и повторной публикации в зеркальный топик.

### Итоговая сборка

| Что                | Источник                                     | Значение без интерфейса |
|--------------------|----------------------------------------------|-------------------------|
| `payload`          | `toPayload()`                                | — (обязателен)          |
| `key`              | `HasKey::getKey()`                           | `null`                  |
| `headers`          | `HasHeaders::getHeaders()` + `withHeader()`  | `[]`                    |
| `partition`        | `HasPartition::getPartition()`               | `RD_KAFKA_PARTITION_UA` |
| `topic`            | маршрут продюсера                           | — (обязателен)          |

## Пакетная публикация

Используйте `Bus::publishBatch()` для отправки нескольких сообщений за один вызов — все они уйдут через один `ProducerStream` с одним `flush()` в конце:

```php
use KafkaBus\Core\Bus\MessageBatch;

$messages = [];

foreach ($products as $product) {
    $messages[] = new ProductCreatedMessage($product->id, $product->name);
}

$bus->publishBatch(MessageBatch::fromArray($messages));
```

::: danger Все сообщения батча должны быть одного класса
`MessageBatch::fromArray()` определяет маршрут по классу **первого** элемента и применяет его ко всему батчу. Если подмешать сообщение другого класса, оно уйдёт не в свой топик — без ошибки и предупреждения (если не подключен стат. анализатор).

Кроме того, `fromArray()` обращается к **первому** элементу напрямую: пустой массив вызовет ошибку. Проверяйте, что список непустой.
:::

Конструктор `MessageBatch` можно вызвать и напрямую, если класс известен заранее — это единственный способ отправить батч из `iterable`, например генератора:

```php
$bus->publishBatch(new MessageBatch(ProductCreatedMessage::class, $generator));
```

`Bus::publish()` — просто частный случай: он оборачивает одно сообщение в `MessageBatch::fromArray([$message])`.

## Переключение подключений

```php
$bus->onConnection('analytics')->publish($message);
```

Таблица маршрутов при этом остаётся общей: `PublisherRoutes` передаётся в `PublisherFactory` один раз и используется всеми подключениями. Меняется только брокер, в который уйдёт сообщение.

## Что дальше

- [Настройка Producer](/ru/docs/producer/configure) — маршруты, flush, параметры
- [Pipeline](/ru/docs/producer/pipeline) — middleware исходящих сообщений
- [Тестирование](/ru/docs/producer/testing) — проверка публикации без брокера
