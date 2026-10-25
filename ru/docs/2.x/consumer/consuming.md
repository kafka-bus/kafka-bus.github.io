---
title: Получение сообщений
description: Обработчики сообщений, автоматическая типизация аргумента и фабрики входящих сообщений Kafka
---

# Получение сообщений

Приём сообщения в Kafka Bus 2.x состоит из двух независимых частей. Воркер читает сырые сообщения из Kafka и вызывает `Bus::dispatch()`. Шина находит маршрут по топику сообщения и вызывает ваш обработчик. Эта страница — о второй части: как написать обработчик и как сообщение превращается в аргумент, который он ожидает.

```php
$bus->dispatch($consumerMessage); // ReceiverRouter → middleware → обработчик
```

Настройка маршрутов — в разделе [Маршруты консьюмера](/ru/docs/2.x/consumer/configure). Запуск цикла чтения — в разделе [Воркеры](/ru/docs/2.x/consumer/workers).

## Обработчики сообщений

Пакет автоматически определяет тип аргумента обработчика и передаёт соответствующее представление сообщения. Обработчик — любой callable: класс с `__invoke` или замыкание.

::: warning
Автоопределение работает через рефлексию и поддерживает только два вида callable: замыкание (`Closure`) и объект с методом `__invoke()`. Для строковых callable вида `'Class::method'` или массива `[$obj, 'method']` тип не определяется — будет использована фабрика по умолчанию (`NativeMessageFactory`).
:::

### Полный доступ к сообщению

```php
use KafkaBus\Core\Consumers\Messages\ConsumerMessageInterface;

class ProductHandler
{
    public function __invoke(ConsumerMessageInterface $message): void
    {
        $payload = $message->payload();   // сырая строка из Kafka
        $headers = $message->headers();   // массив заголовков
        $topic   = $message->topicName(); // физическое имя топика
        $key     = $message->key();       // ключ партиционирования или null
        $id      = $message->msgId();     // "{partition}-{offset}-{topic_name}"
        $raw     = $message->original();  // RdKafka\Message
    }
}
```

Это же представление используется по умолчанию, если тип аргумента распознать не удалось.

::: tip
`key()` возвращает `null`, если ключ пустой: rdkafka отдаёт `''` вместо `null`, и `ConsumerMessage` нормализует это значение.
:::

### Только payload

```php
class ProductHandler
{
    public function __invoke(string $payload): void
    {
        // $payload — сырая строка из Kafka (например, JSON)
        $data = json_decode($payload, true);
    }
}
```

### Payload как декодированный JSON

```php
class ProductHandler
{
    public function __invoke(array $data): void
    {
        // $data — результат json_decode(payload, true)
        echo $data['id'];
    }
}
```

### Оригинальный объект rdkafka

Когда нужен доступ к низкоуровневым метаданным: offset, partition, timestamp:

```php
use RdKafka\Message;

class ProductHandler
{
    public function __invoke(Message $message): void
    {
        echo $message->key;       // ключ партиции Kafka
        echo $message->offset;    // текущий offset
        echo $message->partition; // номер партиции
        echo $message->timestamp; // временная метка
    }
}
```

## Кастомная фабрика сообщений

Атрибут `#[MessageFactory]` позволяет управлять десериализацией payload до того, как он попадёт в обработчик. Это ключевая точка интеграции с [компонентом сообщений](/ru/docs/2.x/components/messages):

```php
use KafkaBus\Core\Receivers\Attributes\MessageFactory;
use KafkaBus\Core\Receivers\Messages\JsonMessageFactory;

class ProductHandler
{
    #[MessageFactory(new JsonMessageFactory())]
    public function __invoke(array $data): void
    {
        // $data — гарантированно декодированный JSON
    }
}
```

Для типизированных доменных сообщений из пакета `kafka-bus/messages`:

```php
use KafkaBus\Messages\Factories\DomainMessageFactory;

class ProductHandler
{
    #[MessageFactory(new DomainMessageFactory(ProductMessage::class))]
    public function __invoke(ProductMessage $message): void
    {
        echo $message->getEvent()->value; // 'create' | 'update' | 'delete'
        echo $message->name;              // типизированное поле
    }
}
```

::: tip Атрибут вешается на `__invoke`
`MessageFactoryExtractor` ищет атрибут на самом методе `__invoke()` (или на теле замыкания), а не на классе. Атрибут объявлен как `Attribute::TARGET_METHOD`.
:::

### Порядок разрешения фабрики

`MessageFactoryExtractor` выбирает фабрику по первому сработавшему правилу:

1. **Атрибут `#[MessageFactory]`** на `__invoke()` — если есть, используется он;
2. **Тип первого аргумента**:
   - `string` → `StringMessageFactory`
   - `array` → `JsonMessageFactory`
   - `RdKafka\Message` → `OriginalMessageFactory`
3. **Фабрика по умолчанию**, переданная в `ReceiverBuilder::make()` / `ReceiverRoutesBuilder::make()` — обычно `NativeMessageFactory`, которая отдаёт сам `ConsumerMessageInterface`.

### Встроенные фабрики

| Фабрика                 | Возвращает                 | Когда применяется                     |
|-------------------------|----------------------------|---------------------------------------|
| `NativeMessageFactory`  | `ConsumerMessageInterface` | По умолчанию                          |
| `StringMessageFactory`  | `string`                   | Аргумент типа `string`                |
| `JsonMessageFactory`    | `array`                    | Аргумент типа `array`                 |
| `OriginalMessageFactory`| `RdKafka\Message`          | Аргумент типа `RdKafka\Message`       |

### Собственная фабрика

Реализуйте `MessageFactoryInterface` — единственный метод получает сообщение и возвращает что угодно:

```php
use KafkaBus\Core\Consumers\Messages\ConsumerMessageInterface;
use KafkaBus\Core\Receivers\Messages\MessageFactoryInterface;

final class ProtobufMessageFactory implements MessageFactoryInterface
{
    public function fromKafka(ConsumerMessageInterface $message): ProductProto
    {
        $proto = new ProductProto();
        $proto->mergeFromString($message->payload());

        return $proto;
    }
}
```

```php
class ProductHandler
{
    #[MessageFactory(new ProtobufMessageFactory())]
    public function __invoke(ProductProto $product): void { /* ... */ }
}
```

## Передача сообщения в шину

`Bus::dispatch()` принимает любой `ConsumerMessageInterface`. В продакшене его вызывает воркер для каждого прочитанного сообщения; можно вызвать и самостоятельно — например, чтобы повторно обработать сообщение из таблицы недоставленных:

```php
use KafkaBus\Core\Consumers\Messages\ConsumerMessage;

$bus->dispatch(new ConsumerMessage($rdkafkaMessage));
```

Что делает `dispatch()`:

1. `ReceiverRouter` находит маршрут по `topicName()` сообщения;
2. фабрика сообщений маршрута превращает сообщение в аргумент обработчика;
3. выполняются middleware маршрута, затем обработчик.

Любое исключение на этом пути — нет маршрута, ошибка фабрики, ошибка обработчика — оборачивается в `MessageConsumerNotHandledException`. Исходное доступно через `getPrevious()`:

```php
use KafkaBus\Core\Exceptions\Consumers\MessageConsumerNotHandledException;

try {
    $bus->dispatch($message);
} catch (MessageConsumerNotHandledException $exception) {
    $exception->consumerMessage; // сообщение, которое не удалось обработать
    $exception->getPrevious();   // исходное исключение
}
```

`dispatch()` не коммитит оффсеты — это задача воркера, и он делает это только после того, как `dispatch()` вернул управление. См. [Коммит оффсетов](/ru/docs/2.x/consumer/workers#коммит-оффсетов).

## Что дальше

- [Маршруты консьюмера](/ru/docs/2.x/consumer/configure) — привязка топиков к обработчикам
- [Воркеры](/ru/docs/2.x/consumer/workers) — чтение Kafka и корректная остановка
- [Pipeline](/ru/docs/2.x/consumer/pipeline) — middleware входящих сообщений
