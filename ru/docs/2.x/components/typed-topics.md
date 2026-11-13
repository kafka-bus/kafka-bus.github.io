---
title: Типизированные топики
description: Несколько типов сообщений в одном топике Kafka с выбором фабрики и обработчика по заголовку x-message-type
---

# Типизированные топики

Используйте типизированные топики, когда несколько форматов событий передаются через один топик Kafka. Продюсер записывает заголовок `x-message-type`, а консьюмер выбирает по нему фабрику сообщения и callback. В этом руководстве настроим два события доставки в одном топике.

## Требования

`kafka-bus/typed-topics` входит в актуальный мета-пакет `kafka-bus/kafka-bus`. При установке отдельных компонентов добавьте его вместе с JSON-классами для примеров ниже:

```bash
composer require kafka-bus/typed-topics kafka-bus/messages
```

Компонент требует PHP 8.2+, `ext-rdkafka` через Core, Kafka Bus Core `^2.2` и `psr/log`. Перед отправкой настройте [подключение](/ru/docs/2.x/configuration).

## Определение типов сообщений

Реализуйте `HasMessageType` и возвращайте стабильный непустой идентификатор типа. Оба сообщения ниже используют JSON:

```php
namespace App\Kafka\Messages;

use KafkaBus\Messages\JsonMessage;
use KafkaBus\TypedTopics\HasMessageType;

final class DeliveryUpdated extends JsonMessage implements HasMessageType
{
    public const TYPE = 'delivery.updated';

    public function getMessageType(): string
    {
        return self::TYPE;
    }
}

final class DeliveryCompleted extends JsonMessage implements HasMessageType
{
    public const TYPE = 'delivery.completed';

    public function getMessageType(): string
    {
        return self::TYPE;
    }
}
```

Разместите каждый класс в отдельном файле в `app/Kafka/Messages/`. Если нужен ключ партиционирования, дополнительно реализуйте `HasKey` из Core: заголовок типа не задаёт ключ.

## Выбор обработчика по типу

Создайте `app/Kafka/Consumers/DeliveryTopicHandler.php`:

```php
namespace App\Kafka\Consumers;

use App\Kafka\Messages\DeliveryCompleted;
use App\Kafka\Messages\DeliveryUpdated;
use KafkaBus\Core\Receivers\Attributes\MessageFactory;
use KafkaBus\Messages\Factories\JsonMessageFactory;
use KafkaBus\TypedTopics\MessageRoutes;
use KafkaBus\TypedTopics\TypedTopicHandler;

final class DeliveryTopicHandler extends TypedTopicHandler
{
    protected function routes(MessageRoutes $routes): void
    {
        $routes
            ->on(DeliveryUpdated::TYPE, $this->onUpdated(...))
            ->on(DeliveryCompleted::TYPE, $this->onCompleted(...));
    }

    #[MessageFactory(new JsonMessageFactory(DeliveryUpdated::class))]
    private function onUpdated(DeliveryUpdated $message): void
    {
        // Примените обновление доставки.
    }

    #[MessageFactory(new JsonMessageFactory(DeliveryCompleted::class))]
    private function onCompleted(DeliveryCompleted $message): void
    {
        // Обработайте результат доставки.
    }
}
```

Callback получает декодированное значение единственным аргументом. Вместо атрибута можно передать фабрику третьим аргументом `on()`. Явная фабрика имеет приоритет; затем проверяется атрибут callback, тип первого параметра (`string`, `array` или `RdKafka\Message`), а по умолчанию передаётся исходное сообщение консьюмера. Повторная регистрация типа вызывает `InvalidArgumentException`.

`TypedTopicHandler` предоставляет фабрику топика через `ProvidesMessageFactory`. Декодирование происходит до запуска middleware консьюмера, поэтому `formatted()` содержит выбранный DTO. Регистрируйте сам объект обработчика: обёртка в виде closure вокруг его `__invoke()` не сохраняет обнаружение фабрики.

## Подключение маршрутов к шине

Для существующего `$connection` свяжите оба класса сообщений с одним топиком и подключите типизированный обработчик:

```php
use App\Kafka\Consumers\DeliveryTopicHandler;
use App\Kafka\Messages\DeliveryCompleted;
use App\Kafka\Messages\DeliveryUpdated;
use KafkaBus\Core\Bus;
use KafkaBus\Core\Publishers\PublisherFactory;
use KafkaBus\Core\Publishers\Routing\Options;
use KafkaBus\Core\Publishers\Routing\PublisherRoutesBuilder;
use KafkaBus\Core\Receivers\Routing\ReceiverBuilder;
use KafkaBus\Core\Receivers\Routing\RouteInfo;
use KafkaBus\Core\Topics\Topic;
use KafkaBus\Core\Topics\TopicRegistry;
use KafkaBus\TypedTopics\PublisherTypedTopicMiddleware;

$topics = (new TopicRegistry())->add(new Topic('delivery.events', 'delivery'));
$options = new Options(middleware: [new PublisherTypedTopicMiddleware()]);

$publisherRoutes = PublisherRoutesBuilder::make($topics)
    ->add(DeliveryUpdated::class, 'delivery', $options)
    ->add(DeliveryCompleted::class, 'delivery', $options)
    ->build();
    
$receiver = ReceiverBuilder::make($topics)
    ->add(new RouteInfo('delivery', new DeliveryTopicHandler()))
    ->build();

$bus = new Bus($connection, new PublisherFactory(routes: $publisherRoutes), $receiver);

$bus->publish(DeliveryUpdated::from(['delivery_id' => 42]));
```

Запись попадёт в `delivery.events` с заголовком `x-message-type: delivery.updated`. Когда [воркер](/ru/docs/2.x/consumer/workers) передаст её этой шине, `onUpdated()` получит экземпляр `DeliveryUpdated`.

::: warning Middleware продюсера обязателен
Вне Laravel добавьте `PublisherTypedTopicMiddleware` в каждый маршрут типизированных сообщений. Одна реализация `HasMessageType` не записывает заголовок.
:::

## Настройка Laravel

Для тех же классов сообщений и обработчика добавьте записи в `config/kafka-bus.php`:

```php
'topics' => [
    'delivery' => 'delivery.events',
],
'producers' => [
    'routes' => [
        App\Kafka\Messages\DeliveryUpdated::class => 'delivery',
        App\Kafka\Messages\DeliveryCompleted::class => 'delivery',
    ],
],
'consumers' => [
    'routes' => [
        'delivery' => App\Kafka\Consumers\DeliveryTopicHandler::class,
    ],
],
```

В актуальном Bridge `PublisherTypedTopicMiddleware` входит в middleware продюсера по умолчанию, а обработчик топика создаётся через контейнер. Отправляйте сообщения через [фасад](/ru/docs/2.x/laravel/producers) и запустите консьюмер:

```bash
php artisan kafka:consume delivery
```

## Обработка записей без подходящего типа

По умолчанию запись без заголовка или с неизвестным типом превращается в `UnroutableMessage`, проходит pipeline консьюмера и завершается без бизнес-обработчика. Коммит оффсета зависит от существующих настроек приложения.

Выберите политику внутри `routes()`:

| Метод                                          | Поведение                                                                                  |
|------------------------------------------------|--------------------------------------------------------------------------------------------|
| `whenHeaderMissing($handler, $factory = null)` | Декодирует записи с отсутствующим или null-заголовком фабрикой обработчика старого формата |
| `onUnroutableLog($logger)`                     | Записывает ошибку через PSR-логгер и завершает обработку                                   |
| `onUnroutableThrow()`                          | Выбрасывает `UnroutableMessageException` через обычный механизм ошибок консьюмера          |
| `onUnroutable($handler)`                       | Передаёт `UnroutableMessage` пользовательскому callback                                    |

Например, после регистрации типов добавьте вызовы, чтобы принимать старые записи и отклонять неизвестные типы:

```php
$routes
    ->whenHeaderMissing($this->onUpdated(...))
    ->onUnroutableThrow();
```

Пустой или неизвестный заголовок не использует обработчик старого формата. `UnroutableMessage` содержит исходное сообщение консьюмера в `message` и причину в `reason` (`MissingHeader` или `UnknownType`). Если callback нужен полученный заголовок, прочитайте его из `$unroutable->message->headers()`.

Исключения фабрики и бизнес-обработчика проходят через обычную [обработку ошибок консьюмера](/ru/docs/2.x/consumer/pipeline). Политика неизвестных типов не перехватывает ошибки декодирования и приложения.

## Тестирование

Для JSON-сообщений из примера используйте `PayloadTestFactory` из `kafka-bus/messages`. Она создаёт плоский JSON payload, который ожидает `JsonMessageFactory`. Фабрики используют Faker; если он ещё не установлен, добавьте зависимость для тестов:

```bash
composer require --dev fakerphp/faker
```

```php
namespace Tests\Factories;

use App\Kafka\Messages\DeliveryUpdated;
use KafkaBus\Messages\Testing\PayloadTestFactory;
use KafkaBus\TypedTopics\HasMessageType;

/**
 * @extends PayloadTestFactory<DeliveryUpdated>
 */
final class DeliveryUpdatedFactory extends PayloadTestFactory
{
    protected string $topicKey = 'delivery';

    protected string $payloadClass = DeliveryUpdated::class;

    /** @var array<string, string> */
    protected array $headers = [
        HasMessageType::HEADER_NAME => DeliveryUpdated::TYPE,
    ];

    public function definition(): array
    {
        return ['delivery_id' => $this->faker->numberBetween(1, 1000)];
    }
}
```

### Подготовка входящей записи

Задайте заголовок типа один раз в свойстве `$headers` фабрики. Вызовите `make()`, чтобы получить `RdKafka\Message`. Метод `payload()` возвращает DTO и не проверяет маршрутизацию входящей записи.

::: info Заголовки по умолчанию
`TestFactory` применяет `$headers` при создании фабрики, поэтому каждый вызов `make()` уже содержит `x-message-type`. Фабрика не запускает pipeline продюсера и не вызывает `HasMessageType::getMessageType()` или `HasKey::getKey()`: тип задаётся в `$headers`, а ключ Kafka — через `withKey()`.
:::

```php
use Tests\Factories\DeliveryUpdatedFactory;

$factory = DeliveryUpdatedFactory::new()
    ->withKey('42');

$record = $factory->make(['delivery_id' => 42]);
```

Для `$receiver` и `$topics` из примера выше передайте реестр в `withTopicKey()`, чтобы преобразовать логический ключ в физическое имя топика:

```php
use KafkaBus\Core\Consumers\Messages\ConsumerMessage;

$record = $factory
    ->withTopicKey('delivery', $topics)
    ->make(['delivery_id' => 42]);

$receiver->dispatch(new ConsumerMessage($record));
```

Запись будет декодирована в `DeliveryUpdated` и передана в `onUpdated()`. В тесте проверяйте результат бизнес-обработчика: например, обновлённую запись в базе или вызов внедрённого сервиса.

### Запуск через Laravel Fake

При конфигурации маршрутов из раздела Laravel можно проверить полный цикл чтения. Fake сам преобразует логический ключ топика с учётом префикса:

```php
use KafkaBus\Laravel\Facades\KafkaBus;
use Tests\Factories\DeliveryUpdatedFactory;

it('consumes a typed delivery update', function () {
    KafkaBus::fake();

    $record = DeliveryUpdatedFactory::new()
        ->withKey('42')
        ->withOffset(0)
        ->make(['delivery_id' => 42]);

    KafkaBus::addMessage($record);
    KafkaBus::listen('delivery');

    KafkaBus::assertCommittedTimes('delivery', 1);
});
```

Проверка коммита подтверждает завершение обработки, но сама по себе не доказывает выбор callback: по умолчанию неизвестные типы тоже завершаются успешно. Дополните тест проверкой результата `onUpdated()`.

Для `DeliveryCompleted` используйте отдельную фабрику с `payloadClass = DeliveryCompleted::class` и значением `DeliveryCompleted::TYPE` в `$headers`. У нескольких записей в одной партиции задавайте разные оффсеты, чтобы защита от дублей не пропустила следующую запись.

Отдельно проверьте запись без заголовка и запись с неизвестным типом: они должны следовать выбранной политике `whenHeaderMissing()` / `onUnroutable()`. `withHeaders()` заменяет весь набор заголовков, поэтому передавайте тип и остальные нужные заголовки одним массивом.


Переопределяйте заголовки через `withHeaders()` для отдельных сценариев тестирования:

```php
use KafkaBus\TypedTopics\HasMessageType;
use Tests\Factories\DeliveryUpdatedFactory;

$withoutType = DeliveryUpdatedFactory::new()->withHeaders([])->make();
$unknownType = DeliveryUpdatedFactory::new()
    ->withHeaders([HasMessageType::HEADER_NAME => 'delivery.unknown'])
    ->make();
```

Для сообщений на базе `DomainMessage` используйте `DomainMessageTestFactory`: она создаёт обёртку `event`, `attributes`, `dirty`. Заголовок типа также задайте в её свойстве `$headers`. Подробнее — [фабрики тестовых сообщений](/ru/docs/2.x/components/messages) и [тестирование в Laravel](/ru/docs/2.x/laravel/testing).
