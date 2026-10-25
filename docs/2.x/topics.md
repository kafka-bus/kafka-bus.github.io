---
title: Topics
description: The topic registry that maps logical keys to physical Kafka topic names
---

# Topics

`TopicRegistry` is a registry that maps a short logical key (e.g. `products`) to the real Kafka topic name (`production.fact.products.1`). Every component — producer, consumer, commiter — works with logical keys, not physical names.

Why this layer of keys exists:

- the physical name changes with the environment and schema version, the logical key doesn't;
- the same code works in `local`, `staging`, and `production` with no changes;
- moving to a new topic version only requires changing one line in the registry.

## Registering topics

```php
use KafkaBus\Core\Topics\Topic;
use KafkaBus\Core\Topics\TopicRegistry;

$topicRegistry = (new TopicRegistry())
    ->add(new Topic('production.fact.products.1', 'products'))
    ->add(new Topic('production.fact.orders.1',   'orders'))
    ->add(new Topic('production.fact.users.1',    'users'));
```

The `Topic` constructor takes two arguments:

```php
new Topic(
    name: 'production.fact.products.1', // physical name in Kafka
    key: 'products',                    // logical key inside the application
)
```

`Topic` is a `readonly` class with public `name` and `key` properties; they can't be changed after construction.

::: warning
The registry is indexed by key, so calling `add()` again with the same `key` silently replaces the previous entry. There's no error — you need to ensure key uniqueness yourself.
:::

## Registry API

| Method                            | Returns    | Behavior if the key isn't found                 |
|-------------------------------------|--------------|-----------------------------------------------------|
| `get(string $topicKey)`          | `Topic`    | Throws `TopicCannotResolvedException`            |
| `getTopicName(string $topicKey)` | `string`   | Throws `TopicCannotResolvedException`            |
| `tryGetTopicName(string $key)`   | `string`   | Returns the given key as-is                       |
| `add(Topic $topic)`              | `self`     | —                                                  |
| `all()`                          | `list<Topic>` | —                                               |

```php
$topicRegistry->get('products')->name;          // 'production.fact.products.1'
$topicRegistry->getTopicName('products');       // 'production.fact.products.1'

$topicRegistry->tryGetTopicName('products');    // 'production.fact.products.1'
$topicRegistry->tryGetTopicName('unknown');     // 'unknown' — no exception
```

`tryGetTopicName()` is used where working with a raw topic name is acceptable — for example, in `BusFaker::addMessage()` and `MessageFactory::withTopicKey()` in tests.

## Naming convention

Recommended format for physical topic names:

```
{env}.{domain}.{entity}.{version}
```

| Segment   | Example                     | Description             |
|-----------|-------------------------------|----------------------------|
| `env`     | `production`, `staging`      | Environment                |
| `domain`  | `fact`, `event`, `command`   | Data type                  |
| `entity`  | `products`, `orders`         | Entity                     |
| `version` | `1`, `2`                      | Schema version              |

Examples of correct names:

```
production.fact.products.1
staging.event.order-created.1
local.command.send-email.1
```

::: warning Topic Versioning
On a breaking schema change, create a new topic with version `2` instead of changing the existing one. This lets consumers migrate gradually.
:::

::: tip Environment Prefix
In the Laravel integration, the `{env}` segment isn't written by hand: the physical name is built from `topic_prefix` + the value from `topics`. See [Laravel Configuration](/docs/2.x/laravel/configuration#topics).
:::

## Using topics in routes

`TopicRegistry` is passed into route builders and workers — the single source of truth for the whole application:

```php
use KafkaBus\Core\Publishers\Routing\PublisherRoutesBuilder;
use KafkaBus\Core\Receivers\Routing\ReceiverBuilder;
use KafkaBus\Core\Receivers\Routing\RouteInfo;
use KafkaBus\Worker\Worker;

// Consumer routes: 'products' is the logical key
$receiver = ReceiverBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', new ProductHandler()))
    ->build();

// Producer routes
$publisherRoutes = PublisherRoutesBuilder::make($topicRegistry)
    ->add(ProductMessage::class, 'products')
    ->build();

// Worker topics
$worker = new Worker('products', [$topicRegistry->get('products')]);
```

Builders resolve the key into a `Topic` when `add()` is called, not when publishing or reading. A typo in a topic key is caught while building routes, not at runtime under load.

## Runtime routing

Once routes are built, keys are no longer used — matching happens by class or by physical name:

- the publisher route collection is keyed by message class → `Route` → `Topic`;
- the receiver route collection is keyed by `Topic::$name`, and `ReceiverRouter` looks up the route by the incoming message's `topicName()`.

Practical consequences:

- if a worker polls a topic that has no receiver route, `dispatch()` fails with a `MessageConsumerNotHandledException` wrapping a `RouteConsumerException`;
- a topic has at most one handler per `Bus`: if two routes point to the same physical topic, only the last one is kept.
