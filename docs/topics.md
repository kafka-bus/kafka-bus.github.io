# Topics

`TopicRegistry` is a registry that maps a short logical key (e.g. `products`) to the real topic name in Kafka (`production.fact.products.1`). All components — producer, consumer, commiter — work with logical keys rather than physical names.

## Registering Topics

```php
use KafkaBus\Core\Topics\Topic;
use KafkaBus\Core\Topics\TopicRegistry;

$topicRegistry = (new TopicRegistry())
    ->add(new Topic('production.fact.products.1', 'products'))
    ->add(new Topic('production.fact.orders.1',   'orders'))
    ->add(new Topic('production.fact.users.1',    'users'));
```

The `Topic` constructor accepts two arguments:

```php
new Topic(
    name: 'production.fact.products.1', // physical name in Kafka
    key: 'products',                    // logical key within the application
)
```

## Naming Convention

Recommended format for physical topic names:

```
{env}.{domain}.{entity}.{version}
```

| Segment   | Example                    | Description     |
|-----------|----------------------------|-----------------|
| `env`     | `production`, `staging`    | Runtime environment |
| `domain`  | `fact`, `event`, `command` | Data type       |
| `entity`  | `products`, `orders`       | Entity          |
| `version` | `1`, `2`                   | Schema version  |

Examples of good names:

```
production.fact.products.1
staging.event.order-created.1
local.command.send-email.1
```

::: warning Topic Versioning
When making a breaking schema change, create a new topic with version `2` instead of modifying the existing one. This allows consumers to migrate gradually.
:::

## Using in Routes

`TopicRegistry` is passed to route builders — it is the single source of truth for the entire application:

```php
// Consumer routes
$consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', new ProductHandler())) // 'products' — logical key
    ->build();

// Publisher routes
$publisherRoutes = PublisherRoutesBuilder::make($topicRegistry)
    ->add(ProductMessage::class, 'products')
    ->build();
```
