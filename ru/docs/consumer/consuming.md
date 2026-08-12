# Получение сообщений

Consumer читает сообщения из Kafka и передаёт их обработчикам. Точка входа — `Bus::listener(string|array $workerName)`, возвращающая listener с блокирующим методом `listen()`.

## Обработчики сообщений

Пакет автоматически определяет тип аргумента обработчика и передаёт соответствующее представление сообщения. Обработчик — любой callable: класс с `__invoke` или замыкание.

::: warning
Автоопределение работает через рефлексию и поддерживает только два вида callable: замыкание (`Closure`) и объект с методом `__invoke()`. Для строковых callable вида `'Class::method'` или массива `[$obj, 'method']` тип не определяется — будет использована фабрика по умолчанию (`NativeMessageFactory`).
:::

### `ConsumerMessageInterface` — полный доступ к сообщению

```php
use KafkaBus\Core\Interfaces\Consumers\Messages\ConsumerMessageInterface;

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

### `string` — только payload

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

### `array` — payload как декодированный JSON

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

### `RdKafka\Message` — оригинальный объект rdkafka

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

Атрибут `#[MessageFactory]` позволяет управлять десериализацией payload до того, как он попадёт в обработчик. Это ключевая точка интеграции с [`kafka-bus/messages`](/ru/docs/components/messages):

```php
use KafkaBus\Core\Consumers\Attributes\MessageFactory;
use KafkaBus\Core\Consumers\Messages\JsonMessageFactory;

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
3. **Фабрика по умолчанию**, переданная в `ConsumerRoutesBuilder::make()` — обычно `NativeMessageFactory`, которая отдаёт сам `ConsumerMessageInterface`.

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
use KafkaBus\Core\Interfaces\Consumers\Messages\ConsumerMessageInterface;
use KafkaBus\Core\Interfaces\Consumers\Messages\MessageFactoryInterface;

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

## Запуск слушателя

```php
$listener = $bus->listener('default');
$listener->listen(); // блокирующий цикл
```

`listen()` не возвращает управление, пока не будет вызван `forceStop()` или не произойдёт неигнорируемая ошибка. Цикл работает так:

1. `Consumer::getMessage()` — ждёт сообщение до `consumerTimeout` мс;
2. `MessageHandler::handle()` — pipeline воркера, маршрутизация, обработчик;
3. `Consumer::commit()` — подтверждение offset.

Коммит выполняется **только после успешного** выполнения обработчика: если обработчик выбросил исключение, оно оборачивается в `MessageConsumerNotHandledException` и всплывает наружу, прерывая цикл.

::: tip Как не ронять воркер на ошибках
Чтобы одно «плохое» сообщение не останавливало обработку, поймайте исключение в middleware воркера — см. [Pipeline консьюмера](/ru/docs/consumer/pipeline#middleware-с-обработкои-ошибок).
:::

### Прослушивание нескольких воркеров

```php
$bus->listener(['products', 'orders'])->listen();
```

Воркеры объединяются в один процесс через `WorkerMerger`, при этом каждый сохраняет собственный набор middleware. Настройки подключения берутся у первого воркера в списке — подробнее в разделе [Архитектура](/ru/docs/architecture#несколько-воркеров-в-одном-процессе).

### Плавная остановка по сигналам

```php
pcntl_async_signals(true);

$listener = $bus->listener('default');

pcntl_signal(SIGINT,  fn () => $listener->forceStop()); // Ctrl+C
pcntl_signal(SIGTERM, fn () => $listener->forceStop()); // kill

$listener->listen();
```

`forceStop()` выставляет флаг, который проверяется в конце итерации цикла. Поэтому остановка произойдёт после того, как текущее сообщение будет обработано и закоммичено — «на полпути» процесс не прервётся.

::: tip
В Laravel это уже сделано за вас: команда `kafka:consume` реализует `SignalableCommandInterface` и обрабатывает `SIGTERM`, `SIGINT` и `SIGQUIT`.
:::
