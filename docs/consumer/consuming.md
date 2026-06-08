# Consumer

Consumer читает сообщения из Kafka и передаёт их в обработчики. Точка входа — `Bus::listener(string $workerName)`, который возвращает слушателя с блокирующим методом `listen()`.

## Обработчики сообщений

Пакет автоматически определяет тип аргумента обработчика и передаёт нужное представление сообщения. Обработчик — это любой callable: класс с `__invoke`, замыкание или метод.

### ConsumerMessageInterface — полный доступ к сообщению

```php
use KafkaBus\Core\Interfaces\Consumers\Messages\ConsumerMessageInterface;

class ProductHandler
{
    public function __invoke(ConsumerMessageInterface $message): void
    {
        $payload = $message->payload();   // строка из Kafka
        $headers = $message->headers();   // массив заголовков
        $topic   = $message->topicName(); // имя топика
    }
}
```

### string — только payload

```php
class ProductHandler
{
    public function __invoke(string $payload): void
    {
        // $payload — raw строка из Kafka (например, JSON)
        $data = json_decode($payload, true);
    }
}
```

### array — payload как декодированный JSON

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

### RdKafka\Message — оригинальный объект rdkafka

Когда нужен доступ к низкоуровневым метаданным: offset, partition, timestamp:

```php
use RdKafka\Message;

class ProductHandler
{
    public function __invoke(Message $message): void
    {
        echo $message->key;       // Kafka partition key
        echo $message->offset;    // текущий offset
        echo $message->partition; // номер партиции
    }
}
```

## Кастомная фабрика сообщений

Атрибут `#[MessageFactory]` позволяет управлять десериализацией payload до передачи в обработчик. Это ключевая точка интеграции с [`kafka-bus-messages`](/packages/messages):

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
        echo $message->name;         // типизированное поле
    }
}
```

## Запуск слушателя

```php
$listener = $bus->listener('default');
$listener->listen(); // блокирующий цикл
```

### Корректная остановка через сигналы

```php
pcntl_async_signals(true);

$listener = $bus->listener('default');

pcntl_signal(SIGINT,  fn () => $listener->forceStop()); // Ctrl+C
pcntl_signal(SIGTERM, fn () => $listener->forceStop()); // kill

$listener->listen();
```
