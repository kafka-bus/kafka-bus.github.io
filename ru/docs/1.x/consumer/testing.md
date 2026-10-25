# Тестирование

`kafka-bus/core` поставляется с fake-заглушками и хелперами, которые позволяют запускать полный pipeline консьюмера без реального брокера Kafka.

::: tip Laravel
В Laravel-приложении сборка не нужна — используйте `KafkaBus::fake()`, `KafkaBus::addMessage()` и `KafkaBus::listen()`. См. [Тестирование в Laravel](/ru/docs/1.x/laravel/testing).
:::

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

Созданное сообщение всегда имеет `err = RD_KAFKA_RESP_ERR_NO_ERROR`, поэтому проходит проверку в `Consumer::getMessage()` как валидное.

::: tip
`withTopicKey()` записывается в поле `topic_name`, но `ConnectionFaker::addMessage()` прогоняет его через `TopicRegistry::tryGetTopicName()`. Поэтому можно передавать как логический ключ (`products`), так и готовое физическое имя — оба варианта сработают.
:::

### Создание сообщения из массива

```php
$message = MessageFactory::for()
    ->withTopicKey('products')
    ->fromArray(['id' => 42, 'name' => 'Ноутбук']);
```

`fromArray()` использует `json_encode()` с флагом `JSON_THROW_ON_ERROR`.

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

::: tip Почему listen() не зависает
`ConsumerFaker` отдаёт сообщения из заранее заполненного массива. Когда сообщения кончаются, он бросает внутреннее `KafkaMessagesEndedException`, а `ConsumerStream` перехватывает его и штатно выходит из цикла. Поэтому `listen()` в тестах не блокирует выполнение — специально останавливать его не нужно.
:::

::: warning
Маршруты здесь создаются напрямую через `new Route(...)`. Обратите внимание, что `Route` консьюмера принимает готовый объект `Topic`, а не ключ, и по умолчанию использует `NativeMessageFactory` — автоопределение по типу аргумента обработчика при этом **не работает**. Чтобы получить поведение как в продакшене, собирайте маршруты через `ConsumerRoutesBuilder`:

```php
use KafkaBus\Core\Consumers\Router\ConsumerRoutesBuilder;
use KafkaBus\Core\Consumers\Router\RouteInfo;

$consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', new YourHandler()))
    ->build();
```
:::

### Проверка закоммиченных сообщений

После возврата `listen()` каждое успешно обработанное сообщение записывается в `$connectionFaker->committedMessages` с ключом — полным именем топика:

```php
$committed = $connectionFaker->committedMessages['production.fact.products.1'];

assert(count($committed) === 1);

$message = $committed[0]; // ConsumerMessageInterface

assert($message->payload() === 'test-message');
assert($message->headers() === ['foo' => 'bar']);
assert($message->topicName() === 'production.fact.products.1');

$original = $message->original(); // RdKafka\Message
assert($original->offset === 0);
```

Сообщение попадает в `committedMessages` только после успешного выполнения обработчика. Если обработчик выбросил исключение — запись отсутствует. Это делает проверку коммитов надёжным индикатором того, что весь pipeline отработал корректно.

## Несколько сообщений

`addMessage()` можно вызывать многократно — сообщения обрабатываются в порядке добавления:

```php
$factory = MessageFactory::for()->withTopicKey('products');

$connectionFaker->addMessage($factory->withOffset(0)->fromArray(['id' => 1]));
$connectionFaker->addMessage($factory->withOffset(1)->fromArray(['id' => 2]));
$connectionFaker->addMessage($factory->withOffset(2)->fromArray(['id' => 3]));

$bus->listener('default-listener')->listen();

assert(count($connectionFaker->committedMessages['production.fact.products.1']) === 3);
```

Иммутабельность билдера здесь особенно удобна: базовая фабрика `$factory` не меняется, а каждый вызов `withOffset()` создаёт новый вариант.

## Тестирование ошибок обработчика

Чтобы проверить поведение при сбое, бросьте исключение из обработчика — оно всплывёт из `listen()` обёрнутым в `MessageConsumerNotHandledException`:

```php
use KafkaBus\Core\Exceptions\Consumers\MessageConsumerNotHandledException;

$consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', fn (string $payload) => throw new RuntimeException('boom')))
    ->build();

try {
    $bus->listener('default-listener')->listen();
    assert(false, 'Ожидалось исключение');
}
catch (MessageConsumerNotHandledException $e) {
    assert($e->getPrevious() instanceof RuntimeException);
}

// Сообщение не закоммичено
assert($connectionFaker->committedMessages === []);
```

Так же проверяется и обратный сценарий: добавьте middleware, поглощающее исключение, и убедитесь, что сообщение всё-таки попало в `committedMessages`.

## Тестирование middleware

Middleware подключаются к воркеру или маршруту точно так же, как в продакшене, — отдельной обвязки не требуется:

```php
$workerRegistry = (new MemoryWorkerRegistry())
    ->add(new Worker(
        name:    'default-listener',
        routes:  $consumerRoutes,
        options: new Options(middleware: [new ErrorHandlingMiddleware($logger)]),
    ));
```

Поскольку `committedMessages` заполняется только при успешном проходе, факт коммита сам по себе подтверждает, что цепочка middleware отработала до конца.
