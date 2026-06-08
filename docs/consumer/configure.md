# Configure

## Consumption Flow

```
Application
    │
    ▼ listener(workerName)
  Bus → Thread → ListenerFactory
    │
    ▼ listen()
  Listener
    │
    ▼
  ConsumerStream → Kafka Broker
    │ ← message
    ▼
  Consumer Pipeline
    │
    ▼
  ConsumerRouter → Route Pipeline → MessageHandler (business logic)
```

In detail:

1. The application requests a listener: `Bus::listener('worker-name')`.
2. `Bus` delegates to `Thread`, which uses `ListenerFactory` to create a `Listener`.
3. `Listener` starts `ConsumerStream` — a blocking read loop.
4. Each message passes through the Consumer Pipeline.
5. `ConsumerRouter` finds the appropriate `MessageHandler` by topic name.
6. The message is converted via `MessageFactory`.
7. The message is passed to the Route Pipeline.
8. The handler executes the business logic.

## Routes

`ConsumerRoutesBuilder` maps a logical topic key to a handler:

```php
use KafkaBus\Core\Consumers\Router\ConsumerRoutesBuilder;
use KafkaBus\Core\Consumers\Router\RouteInfo;

$consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', new ProductHandler()))
    ->add(new RouteInfo('orders',   new OrderHandler()))
    ->build();
```

### Route with Middleware

```php
use KafkaBus\Core\Consumers\Router\RouteOptions;

$consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry)
    ->add(new RouteInfo(
        topicKey: 'orders',
        handler:  new OrderHandler(),
        options:  new RouteOptions(middleware: [new TenantMiddleware()])
    ))
    ->build();
```

## Workers

A worker is a named consumer configuration. A single worker can listen to multiple topics:

```php
use KafkaBus\Core\Bus\Listeners\Workers\MemoryWorkerRegistry;
use KafkaBus\Core\Bus\Listeners\Workers\Worker;
use KafkaBus\Core\Bus\Listeners\Workers\Options;

$workerRegistry = MemoryWorkerRegistry::make()
    ->add(new Worker(
        name:    'default',
        routes:  $consumerRoutes,
        options: new Options(
            middleware:        [],
            additionalOptions: [
                'group.id'              => 'my-service',
                'auto.offset.reset'     => 'earliest',
                'max.poll.interval.ms'  => 300_000,
                'session.timeout.ms'    => 45_000,
                'heartbeat.interval.ms' => 3_000,
            ],
            autoCommit:       false,
            consumeTimeout:   5_000,
        )
    ));
```

## Consumer Options

| librdkafka option       | Description                  | Recommended value       |
|-------------------------|------------------------------|-------------------------|
| `group.id`              | Consumer group name          | Your service name       |
| `auto.offset.reset`     | Position on first start      | `earliest` or `latest`  |
| `max.poll.interval.ms`  | Max time between polls       | `300000`                |
| `session.timeout.ms`    | Session timeout              | `45000`                 |
| `heartbeat.interval.ms` | Heartbeat interval           | `3000`                  |
| `enable.auto.commit`    | Auto-commit offset           | `false` (recommended)   |
