# Consumer

Consumer читает сообщения из Kafka и передаёт их в обработчики. Точка входа — `Bus::listener(string $workerName)`, который возвращает слушателя с блокирующим методом `listen()`.

## Обработчики сообщений

Пакет автоматически определяет тип аргумента обработчика и передаёт нужное представление сообщения. Обработчик — это любой callable: класс с `__invoke`, замыкание или метод.

### ConsumerMessageInterface — полный доступ к сообщению

```php
use Micromus\KafkaBus\Interfaces\Consumers\Messages\ConsumerMessageInterface;

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
use Micromus\KafkaBus\Consumers\Attributes\MessageFactory;
use Micromus\KafkaBus\Consumers\Messages\JsonMessageFactory;

class ProductHandler
{
    #[MessageFactory(new JsonMessageFactory())]
    public function __invoke(array $data): void
    {
        // $data — гарантированно декодированный JSON
    }
}
```

Для типизированных доменных сообщений из пакета `kafka-bus-messages`:

```php
use Micromus\KafkaBusMessages\Factories\DomainMessageFactory;

class ProductHandler
{
    #[MessageFactory(new DomainMessageFactory(ProductMessage::class))]
    public function __invoke(ProductMessage $message): void
    {
        echo $message->event->value; // 'create' | 'update' | 'delete'
        echo $message->name;         // типизированное поле
    }
}
```

## Маршруты consumer

`ConsumerRoutesBuilder` связывает логический ключ топика с обработчиком:

```php
use Micromus\KafkaBus\Consumers\Router\ConsumerRoutesBuilder;
use Micromus\KafkaBus\Consumers\Router\RouteInfo;

$consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', new ProductHandler()))
    ->add(new RouteInfo('orders',   new OrderHandler()))
    ->build();
```

### Маршрут с middleware

```php
use Micromus\KafkaBus\Consumers\Router\RouteOptions;

$consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry)
    ->add(new RouteInfo(
        topicKey: 'orders',
        handler:  new OrderHandler(),
        options:  new RouteOptions(middleware: [new TenantMiddleware()])
    ))
    ->build();
```

## Воркеры

Воркер — именованная конфигурация consumer'а. Один воркер может слушать несколько топиков:

```php
use Micromus\KafkaBus\Bus\Listeners\Workers\MemoryWorkerRegistry;
use Micromus\KafkaBus\Bus\Listeners\Workers\Worker;
use Micromus\KafkaBus\Bus\Listeners\Workers\Options;

$workerRegistry = MemoryWorkerRegistry::make()
    ->add(new Worker(
        name:    'default',
        routes:  $consumerRoutes,
        options: new Options(
            middleware:        [],
            additionalOptions: [
                'group.id'              => 'my-service',
                'auto.offset.reset'     => 'earliest',
                'max.poll.interval.ms'  => 300_000,
                'session.timeout.ms'    => 45_000,
                'heartbeat.interval.ms' => 3_000,
            ],
            autoCommit:       false,
            consumeTimeout:   5_000,
        )
    ));
```

## Запуск слушателя

```php
$listener = $bus->listener('default');
$listener->listen(); // блокирующий цикл
```

### Корректная остановка через сигналы

```php
if (extension_loaded('pcntl')) {
    pcntl_async_signals(true);

    $listener = $bus->listener('default');

    pcntl_signal(SIGINT,  fn () => $listener->forceStop()); // Ctrl+C
    pcntl_signal(SIGTERM, fn () => $listener->forceStop()); // kill

    $listener->listen();
}
```

## Опции consumer

| Опция librdkafka | Описание | Рекомендуемое значение |
|---|---|---|
| `group.id` | Имя consumer group | Имя вашего сервиса |
| `auto.offset.reset` | Позиция при первом старте | `earliest` или `latest` |
| `max.poll.interval.ms` | Макс. время между poll | `300000` |
| `session.timeout.ms` | Таймаут сессии | `45000` |
| `heartbeat.interval.ms` | Интервал heartbeat | `3000` |
| `enable.auto.commit` | Авто-коммит offset | `false` (рекомендуется) |
