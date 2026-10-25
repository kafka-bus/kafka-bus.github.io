---
title: Consumer routes
description: Binding topics to handlers with ReceiverBuilder, route middleware, lazy receivers, and the default message factory
---

# Consumer routes

A consumer route binds a topic to a handler. Routes are collected into a receiver, and the receiver is passed into `Bus`. When a message is dispatched, the bus looks up the route by the message's topic and runs its handler.

Routes are independent of workers: a worker decides which topics to read, a route decides what to do with a message from a topic. See [Workers](/docs/2.x/consumer/workers) for the reading side.

## Processing flow

```
Worker (kafka-bus/worker)
    │ getMessage()
    ▼
  Bus::dispatch(ConsumerMessageInterface)
    ▼
  Receiver → ReceiverRouter
    │ finds the Route by physical topic name
    ▼
  RouteExecutor
    │ → MessageFactory (deserialization)
    │ → ReceiverPipelineHandler + route middleware
    ▼
  Handler (business logic)
    │
    ▼ back in the worker: commit()
```

## Building a receiver

`ReceiverBuilder` maps logical topic keys to handlers and builds a `Receiver`:

```php
use KafkaBus\Core\Bus;
use KafkaBus\Core\Receivers\Routing\ReceiverBuilder;
use KafkaBus\Core\Receivers\Routing\RouteInfo;

$receiver = ReceiverBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', new ProductHandler()))
    ->add(new RouteInfo('orders', new OrderHandler()))
    ->build();

$bus = new Bus(
    connection: $connection,
    receiver: $receiver,
);
```

`ReceiverBuilder` methods:

| Method | Returns | Description |
|---|---|---|
| `make(TopicRegistry $registry, ?MessageFactoryInterface $factory = null)` | `ReceiverBuilder` | Creates a builder; `$factory` is the default message factory |
| `add(RouteInfo $routeInfo)` | `self` | Adds a route; the topic key is resolved immediately |
| `build()` | `Receiver` | Builds the receiver right away |
| `lazy()` | `LazyReceiver` | Defers building until the first message, see [Lazy receiver](#lazy-receiver) |
| `fake()` | `ReceiverFaker` | Builds a receiver that also records dispatched messages, for tests |

::: warning One handler per topic
Routes are keyed by the physical topic name. Adding a second route for the same topic replaces the first one — a topic has exactly one handler inside a `Bus`.
:::

### Route parameters

```php
new RouteInfo(
    topicKey: 'orders',             // logical topic key
    handler: new OrderHandler(),    // any callable
    middleware: [],                 // list<ReceiverPipelineMiddleware>
);
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `topicKey` | `string` | — | Topic key from `TopicRegistry` |
| `handler` | `callable` | — | The message handler |
| `middleware` | `list<ReceiverPipelineMiddleware>` | `[]` | Middleware for this route |

### Route with middleware

```php
$receiver = ReceiverBuilder::make($topicRegistry)
    ->add(new RouteInfo(
        topicKey: 'orders',
        handler: new OrderHandler(),
        middleware: [new LoggingMiddleware($logger), new TenantMiddleware()],
    ))
    ->build();
```

There is no "global" consumer middleware at the core level: list shared middleware in each route. See [Consumer pipeline](/docs/2.x/consumer/pipeline).

### Default message factory

The second argument of `ReceiverBuilder::make()` sets the factory used when the handler's argument type can't be detected:

```php
use KafkaBus\Core\Receivers\Messages\StringMessageFactory;

$receiver = ReceiverBuilder::make($topicRegistry, new StringMessageFactory())
    ->add(new RouteInfo('products', $legacyHandler))
    ->build();
```

Without the second argument, `NativeMessageFactory` is used. Resolution priority is described in [Consuming messages](/docs/2.x/consumer/consuming#factory-resolution-order).

## Lazy receiver

Building a receiver instantiates every handler and middleware up front. If handlers have heavy dependencies and the process mostly publishes, defer this work with `lazy()`:

```php
$bus = new Bus(
    connection: $connection,
    receiver: ReceiverBuilder::make($topicRegistry)
        ->add(new RouteInfo('products', new ProductHandler()))
        ->lazy(),
);
```

`LazyReceiver` builds the real receiver on the first `dispatch()` and reuses it afterwards. For finer control, a route collection can also hold individual `LazyRoute` objects — see [Routes and lazy resolution](/docs/2.x/architecture#routes-and-lazy-resolution).

## Low-level route collection

`ReceiverBuilder` is a thin wrapper over `ReceiverRoutesBuilder` and `ReceiverRouter`. To build the collection yourself:

```php
use KafkaBus\Core\Receivers\Receiver;
use KafkaBus\Core\Receivers\Routing\ReceiverRouter;
use KafkaBus\Core\Receivers\Routing\ReceiverRoutesBuilder;

$routes = ReceiverRoutesBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', new ProductHandler()))
    ->build(); // RouteCollection<Route>

$receiver = new Receiver(new ReceiverRouter($routes));
```

::: warning
A `Receivers\Routing\Route` created directly with `new Route(...)` uses `NativeMessageFactory` by default — argument type detection only happens in `ReceiverRoutesBuilder::add()`.
:::
