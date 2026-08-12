# Тестирование

`kafka-bus/core` предоставляет fake-заглушки, позволяющие проверять поведение продюсера без реального брокера Kafka.

::: tip Laravel
В Laravel-приложении ничего собирать вручную не нужно — используйте `KafkaBus::fake()` и готовые ассерты `assertPublished()` / `assertPublishedTimes()`. См. [Тестирование в Laravel](/ru/docs/laravel/testing).
:::

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

`ProducerMessageFaker` не реализует `HasKey`, поэтому ключ опубликованного сообщения всегда `null`. Если нужно проверить логику ключа партиционирования, используйте собственный класс сообщения.

## Полный тест продюсера

`ConnectionFaker` записывает каждое опубликованное сообщение в память. `ConnectionRegistryFaker` подключает его к `Bus` вместо реального соединения — причём возвращает одно и то же подключение для любого имени, так что `onConnection()` в тестах тоже работает.

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

::: tip
Маршруты здесь собираются напрямую через `new Route(...)`, минуя `PublisherRoutesBuilder`. Это удобно в тестах: не нужно, чтобы ключ топика был зарегистрирован в реестре — достаточно готового объекта `Topic`.
:::

### Проверка опубликованных сообщений

После `publish()` все отправленные сообщения доступны в `$connectionFaker->publishedMessages` с ключом — полным именем топика:

```php
$published = $connectionFaker->publishedMessages['production.fact.products.1'];

assert(count($published) === 1);

$message = $published[0]; // KafkaBus\Core\Producers\Messages\ProducerMessage

assert($message->payload   === 'test-message');
assert($message->partition === 5);
assert($message->headers   === ['foo' => 'bar']);
assert($message->key       === null);
assert($message->topic->key === 'products');
```

::: warning
В `publishedMessages` попадают объекты `ProducerMessage` — результат работы **всего** pipeline, а не исходные сообщения приложения. Это удобно: в тесте видно ровно то, что ушло бы в брокер, включая заголовки, добавленные middleware. Но это не `RdKafka\Message` — у него нет `offset`, `err` и прочих полей брокера.
:::

Доступные свойства `ProducerMessage` (все `readonly`):

| Свойство    | Тип                    | Описание                              |
|-------------|------------------------|---------------------------------------|
| `topic`     | `Topic`                | Целевой топик                         |
| `payload`   | `string`               | Результат `toPayload()`               |
| `headers`   | `array<string, mixed>` | Итоговые заголовки                    |
| `partition` | `int`                  | `-1`, если партиция не задана явно    |
| `key`       | `?string`              | Ключ партиционирования                |

## Проверка middleware

Поскольку в `publishedMessages` лежит результат всего pipeline, middleware проверяются без отдельной обвязки — достаточно добавить их в `Options` маршрута:

```php
use KafkaBus\Core\Bus\Publishers\Router\Options;
use KafkaBus\Commiter\Middleware\ProducerIdempotencyMiddleware;

$routes = (new PublisherRoutes())
    ->add(new Route(
        OrderMessage::class,
        $topicRegistry->get('orders'),
        new Options(middleware: [new ProducerIdempotencyMiddleware()])
    ));

$bus->publish(new OrderMessage(orderId: 42));

$published = $connectionFaker->publishedMessages['production.fact.orders.1'];

assert($published[0]->headers['x-idempotency-key'] === 'order-42');
```

Аналогично проверяется и отмена публикации: если middleware не вызвал `continue()`, массив `publishedMessages` для этого топика останется пустым.

## Проверка отсутствия публикации

`ConnectionFaker::$publishedMessages` — обычный публичный массив, поэтому отрицательные проверки пишутся напрямую:

```php
assert($connectionFaker->publishedMessages === []);                  // ничего не публиковалось
assert(! isset($connectionFaker->publishedMessages['other.topic'])); // в этот топик не писали
```

В реальном тесте вместо `ProducerMessageFaker` используйте собственную реализацию `ProducerMessageInterface` и проверяйте маршрутизацию, сериализацию payload и формирование заголовков.
