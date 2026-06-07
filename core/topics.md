# Топики

`TopicRegistry` — реестр, который связывает короткий логический ключ (например `products`) с реальным именем топика в Kafka (`production.fact.products.1`). Все компоненты — producer, consumer, commiter — работают с логическими ключами, а не с физическими именами.

## Регистрация топиков

```php
use Micromus\KafkaBus\Topics\Topic;
use Micromus\KafkaBus\Topics\TopicRegistry;

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

## Соглашение об именовании

Рекомендуемый формат физического имени топика:

```
{env}.{domain}.{entity}.{version}
```

| Сегмент | Пример | Описание |
|---|---|---|
| `env` | `production`, `staging` | Среда запуска |
| `domain` | `fact`, `event`, `command` | Тип данных |
| `entity` | `products`, `orders` | Сущность |
| `version` | `1`, `2` | Версия схемы |

Примеры хороших имён:

```
production.fact.products.1
staging.event.order-created.1
local.command.send-email.1
```

::: warning Версионирование топиков
При несовместимом изменении схемы сообщения создавайте новый топик с версией `2`, а не меняйте существующий. Это позволит мигрировать потребителей постепенно.
:::

## Префиксы в Laravel

В Laravel-пакете физическое имя топика собирается автоматически из `topic_prefix` и значения в `topics`:

```php
// config/kafka-bus.php
'topic_prefix' => env('KAFKA_PREFIX', env('APP_ENV', 'local') . '.'),

'topics' => [
    'products' => 'fact.products.1',
    'orders'   => 'fact.orders.1',
],
```

При `APP_ENV=production`:
- `products` → `production.fact.products.1`
- `orders` → `production.fact.orders.1`

Префикс можно переопределить через ENV без изменения конфига:

```dotenv
KAFKA_PREFIX=staging.
```

## Использование в маршрутах

`TopicRegistry` передаётся в билдеры маршрутов — это единая точка истины для всего приложения:

```php
// Consumer routes
$consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', new ProductHandler())) // 'products' — логический ключ
    ->build();

// Publisher routes
$publisherRoutes = PublisherRoutesBuilder::make($topicRegistry)
    ->add(ProductMessage::class, 'products')
    ->build();
```
