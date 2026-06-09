# Тестирование

`kafka-bus/core` поставляется с fake-заглушками и хелперами, которые позволяют запускать полный pipeline консьюмера без реального брокера Kafka.

## MessageFactory

`MessageFactory` создаёт объекты `RdKafka\Message` для использования в тестах. Следует паттерну иммутабельного билдера — каждый вызов `with*` возвращает клон, поэтому один экземпляр фабрики может создавать множество вариаций.

```php
use KafkaBus\Core\Testing\Consumers\MessageFactory;

$message = MessageFactory::for()
    ->withTopicKey('products')
    ->withHeaders(['x-idempotency-key' => 'prod-42'])
    ->make('{"id":42,"name":"Ноутбук"}');
```

### Доступные методы

| Метод                           | По умолчанию | Описание                                 |
|---------------------------------|--------------|------------------------------------------|
| `withTopicKey(string $key)`     | `'test'`     | Устанавливает `topic_name` сообщения     |
| `withHeaders(array $headers)`   | `[]`         | Устанавливает заголовки сообщения        |
| `withKey(?string $key)`         | `null`       | Устанавливает ключ партиционирования     |
| `withPartition(int $partition)` | `0`          | Устанавливает номер партиции             |
| `withOffset(int $offset)`       | `0`          | Устанавливает offset                     |
| `make(string $payload)`         | —            | Возвращает настроенный `RdKafka\Message` |
| `fromArray(array $attributes)`  | —            | JSON-кодирует массив и вызывает `make()` |

### Создание сообщения из массива

```php
$message = MessageFactory::for()
    ->withTopicKey('products')
    ->fromArray(['id' => 42, 'name' => 'Ноутбук']);
```

## Полный тест консьюмера

Пример ниже — интеграционный тест без привязки к фреймворку. `ConnectionFaker` и `ConnectionRegistryFaker` заменяют реальный брокер — весь pipeline (маршрутизация, обработчик, коммит) выполняется целиком в памяти.

```php
use KafkaBus\Core\Bus;
use KafkaBus\Core\Bus\Listeners\ListenerFactory;
use KafkaBus\Core\Bus\Listeners\Workers\MemoryWorkerRegistry;
use KafkaBus\Core\Bus\Listeners\Workers\Worker;
use KafkaBus\Core\Bus\Publishers\PublisherFactory;
use KafkaBus\Core\Bus\ThreadFactory;
use KafkaBus\Core\Bus\ThreadRegistry;
use KafkaBus\Core\Consumers\ConsumerStreamFactory;
use KafkaBus\Core\Consumers\Handlers\MessageHandlerFactory;
use KafkaBus\Core\Consumers\Router\ConsumerRoutes;
use KafkaBus\Core\Consumers\Router\Route;
use KafkaBus\Core\Producers\ProducerStreamFactory;
use KafkaBus\Core\Testing\Connections\ConnectionFaker;
use KafkaBus\Core\Testing\Connections\ConnectionRegistryFaker;
use KafkaBus\Core\Testing\Consumers\MessageFactory;
use KafkaBus\Core\Topics\Topic;
use KafkaBus\Core\Topics\TopicRegistry;

// 1. Регистрация топиков
$topicRegistry = (new TopicRegistry())
    ->add(new Topic('production.fact.products.1', 'products'));

$connectionFaker = new ConnectionFaker($topicRegistry);

// 2. Добавление тестового сообщения в fake-подключение
$connectionFaker->addMessage(
    MessageFactory::for()
        ->withHeaders(['foo' => 'bar'])
        ->withTopicKey('products')
        ->make('test-message')
);

// 3. Настройка маршрутов и обработчиков
$consumerRoutes = (new ConsumerRoutes())
    ->add(new Route(
        topic: $topicRegistry->get('products'),
        handler: new YourHandler(),
    ));

$workerRegistry = (new MemoryWorkerRegistry())
    ->add(new Worker('default-listener', $consumerRoutes));

// 4. Сборка Bus с fake-подключениями
$bus = new Bus(
    new ThreadRegistry(
        new ConnectionRegistryFaker($connectionFaker),
        new ThreadFactory(
            new ListenerFactory(
                new ConsumerStreamFactory(new MessageHandlerFactory()),
                $workerRegistry
            ),
            new PublisherFactory(new ProducerStreamFactory())
        )
    ),
    'default'
);

// 5. Запуск полного pipeline
$bus->listener('default-listener')->listen();
```

### Проверка закоммиченных сообщений

После возврата `listen()` каждое успешно обработанное сообщение записывается в `$connectionFaker->committedMessages` с ключом — полным именем топика:

```php
$committed = $connectionFaker->committedMessages['production.fact.products.1'];

assert(count($committed) === 1);

$original = $committed[0]->original(); // RdKafka\Message

assert($original->payload === 'test-message');
assert($original->headers === ['foo' => 'bar']);
```

Сообщение попадает в `committedMessages` только после успешного выполнения обработчика. Если обработчик выбросил исключение — запись отсутствует. Это делает проверку коммитов надёжным индикатором того, что весь pipeline отработал корректно.