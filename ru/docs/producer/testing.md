# Тестирование

`kafka-bus/core` предоставляет fake-заглушки, позволяющие проверять поведение продюсера без реального брокера Kafka.

## ProducerMessageFaker

`ProducerMessageFaker` — лёгкая реализация `ProducerMessageInterface` для тестов. Реализует `HasHeaders` и `HasPartition`, поэтому можно управлять всеми параметрами исходящего сообщения.

```php
use KafkaBus\Core\Testing\Messages\ProducerMessageFaker;

$message = new ProducerMessageFaker(
    message:   'test-message',       // строка payload
    headers:   ['foo' => 'bar'],     // опционально
    partition: 5,                    // опционально, -1 = автовыбор
);
```

| Параметр     | Тип      | По умолчанию | Описание                                            |
|--------------|----------|--------------|-----------------------------------------------------|
| `$message`   | `string` | —            | Raw payload, возвращаемый `toPayload()`             |
| `$headers`   | `array`  | `[]`         | Заголовки, возвращаемые `getHeaders()`              |
| `$partition` | `int`    | `-1`         | Номер партиции; `-1` — Kafka выбирает автоматически |

## Полный тест продюсера

`ConnectionFaker` записывает каждое опубликованное сообщение в память. `ConnectionRegistryFaker` подключает его к `Bus` вместо реального соединения.

```php
use KafkaBus\Core\Bus;
use KafkaBus\Core\Bus\Listeners\ListenerFactory;
use KafkaBus\Core\Bus\Publishers\PublisherFactory;
use KafkaBus\Core\Bus\Publishers\Router\PublisherRoutes;
use KafkaBus\Core\Bus\Publishers\Router\Route;
use KafkaBus\Core\Bus\ThreadFactory;
use KafkaBus\Core\Bus\ThreadRegistry;
use KafkaBus\Core\Consumers\ConsumerStreamFactory;
use KafkaBus\Core\Producers\ProducerStreamFactory;
use KafkaBus\Core\Testing\Connections\ConnectionFaker;
use KafkaBus\Core\Testing\Connections\ConnectionRegistryFaker;
use KafkaBus\Core\Testing\Messages\ProducerMessageFaker;
use KafkaBus\Core\Topics\Topic;
use KafkaBus\Core\Topics\TopicRegistry;

// 1. Регистрация топиков
$topicRegistry = (new TopicRegistry())
    ->add(new Topic('production.fact.products.1', 'products'));

$connectionFaker = new ConnectionFaker($topicRegistry);

// 2. Маппинг классов сообщений на топики
$routes = (new PublisherRoutes())
    ->add(new Route(ProducerMessageFaker::class, $topicRegistry->get('products')));

// 3. Сборка Bus с fake-подключениями
$bus = new Bus(
    new ThreadRegistry(
        new ConnectionRegistryFaker($connectionFaker),
        new ThreadFactory(
            new ListenerFactory(
                new ConsumerStreamFactory(),
            ),
            new PublisherFactory(
                new ProducerStreamFactory(),
                $routes
            ),
        )
    ),
    'default'
);

// 4. Публикация сообщения
$bus->publish(new ProducerMessageFaker('test-message', ['foo' => 'bar'], 5));
```

### Проверка опубликованных сообщений

После `publish()` все отправленные сообщения доступны в `$connectionFaker->publishedMessages` с ключом — полным именем топика:

```php
$published = $connectionFaker->publishedMessages['production.fact.products.1'];

assert(count($published) === 1);

$message = $published[0]; // объект, совместимый с RdKafka

assert($message->payload   === 'test-message');
assert($message->partition === 5);
assert($message->headers   === ['foo' => 'bar']);
```

В реальном тесте вместо `ProducerMessageFaker` можно использовать собственную реализацию `ProducerMessageInterface` и проверять маршрутизацию, сериализацию payload и формирование заголовков.