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
