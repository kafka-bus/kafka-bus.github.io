# Топики

`TopicRegistry` — реестр, который маппит короткий логический ключ (например, `products`) на реальное имя топика в Kafka (`production.fact.products.1`). Все компоненты — продюсер, консьюмер, commiter — работают с логическими ключами, а не с физическими именами.

Зачем нужен слой ключей:

- физическое имя меняется вместе с окружением и версией схемы, логический ключ — нет;
- один и тот же код работает в `local`, `staging` и `production` без правок;
- при переходе на новую версию топика достаточно поменять одну строку в реестре.

## Регистрация топиков

```php
use KafkaBus\Core\Topics\Topic;
use KafkaBus\Core\Topics\TopicRegistry;

$topicRegistry = (new TopicRegistry())
    ->add(new Topic('production.fact.products.1', 'products'))
    ->add(new Topic('production.fact.orders.1',   'orders'))
    ->add(new Topic('production.fact.users.1',    'users'));
```

Конструктор `Topic` принимает два аргумента:

```php
new Topic(
    name: 'production.fact.products.1', // физическое имя в Kafka
    key: 'products',                    // логический ключ внутри приложения
)
```

`Topic` — `readonly`-класс с публичными свойствами `name` и `key`; изменить их после создания нельзя.

::: warning
Реестр индексируется по ключу, поэтому повторный `add()` с тем же `key` молча заменит предыдущую запись. Ошибки не будет — проверяйте уникальность ключей сами.
:::

## API реестра

| Метод                            | Возвращает | Поведение, если ключ не найден                  |
|----------------------------------|------------|--------------------------------------------------|
| `get(string $topicKey)`          | `Topic`    | Бросает `TopicCannotResolvedException`           |
| `getTopicName(string $topicKey)` | `string`   | Бросает `TopicCannotResolvedException`           |
| `tryGetTopicName(string $key)`   | `string`   | Возвращает переданный ключ как есть              |
| `add(Topic $topic)`              | `self`     | —                                                |

```php
$topicRegistry->get('products')->name;          // 'production.fact.products.1'
$topicRegistry->getTopicName('products');       // 'production.fact.products.1'

$topicRegistry->tryGetTopicName('products');    // 'production.fact.products.1'
$topicRegistry->tryGetTopicName('unknown');     // 'unknown' — без исключения
```

`tryGetTopicName()` используется там, где допустимо работать с сырым именем топика — например, в `ConnectionFaker` при добавлении тестовых сообщений.

## Соглашение об именовании

Рекомендуемый формат физических имён топиков:

```
{env}.{domain}.{entity}.{version}
```

| Сегмент   | Пример                     | Описание               |
|-----------|----------------------------|------------------------|
| `env`     | `production`, `staging`    | Окружение              |
| `domain`  | `fact`, `event`, `command` | Тип данных             |
| `entity`  | `products`, `orders`       | Сущность               |
| `version` | `1`, `2`                   | Версия схемы           |

Примеры правильных имён:

```
production.fact.products.1
staging.event.order-created.1
local.command.send-email.1
```

::: warning Версионирование топиков
При несовместимом изменении схемы создавайте новый топик с версией `2` вместо изменения существующего. Это позволяет консьюмерам мигрировать постепенно.
:::

::: tip Префикс окружения
В интеграции с Laravel сегмент `{env}` не пишется руками: физическое имя собирается как `topic_prefix` + значение из `topics`. См. [Конфигурация Laravel](/ru/docs/1.x/laravel/configuration#топики).
:::

## Использование в маршрутах

`TopicRegistry` передаётся в билдеры маршрутов — единый источник истины для всего приложения:

```php
// Маршруты консьюмера
$consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', new ProductHandler())) // 'products' — логический ключ
    ->build();

// Маршруты продюсера
$publisherRoutes = PublisherRoutesBuilder::make($topicRegistry)
    ->add(ProductMessage::class, 'products')
    ->build();
```

Оба билдера резолвят ключ в `Topic` **в момент вызова `add()`**, а не при публикации или чтении. Поэтому опечатка в ключе топика обнаружится на этапе сборки маршрутов, а не в рантайме под нагрузкой.

## Маршрутизация во время выполнения

После сборки маршрутов ключи больше не используются — сопоставление идёт по физическим именам:

- `PublisherRoutes` индексируются по классу сообщения → `Route` → `Topic`;
- `ConsumerRoutes` индексируются по `topic->name`, и `ConsumerRouter` ищет маршрут по `$message->topicName()` из входящего сообщения.

Отсюда практическое следствие: если воркер подписан на топик, для которого нет маршрута, `ConsumerRouter` бросит `RouteConsumerException`. Такое возможно, когда несколько маршрутов указывают на один и тот же физический топик — в `ConsumerRoutes` останется только последний.
