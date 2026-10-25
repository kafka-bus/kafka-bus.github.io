---
title: Architecture
description: How Kafka Bus 2.x is built — the bus, publisher and receiver routers, workers, pipelines, and error handling
---

# Architecture

This page explains how the parts of Kafka Bus fit together: what `Bus` does, how a message travels in each direction, and why reading from Kafka is handled by a separate worker. Read it when you need to understand the behavior behind the API rather than the API itself.

## Key concepts

| Concept | Class / interface | Package | Description |
|---|---|---|---|
| **Bus** | `BusInterface`, `Bus` | `core` | The single entry point: `publish()` out, `dispatch()` in |
| **Connection** | `ConnectionInterface` | `core` | A configured connection to a Kafka cluster |
| **Topic** | `Topics\Topic` | `core` | A physical topic name with a logical key |
| **Publisher** | `Publishers\Publisher` | `core` | Routes outgoing messages by class to a topic |
| **Receiver** | `Receivers\ReceiverInterface` | `core` | Routes incoming messages by topic to a handler |
| **Pipeline** | `Pipelines\PipelineInterface` | `core` | Middleware chain around publishing and handling |
| **Worker** | `KafkaBus\Worker\Worker` | `worker` | A named list of topics to poll and consumer options |
| **Metadata** | `KafkaBus\Metadata\Metadata` | `metadata` | Cluster topics, partitions, and consumer group offsets |

## Symmetric bus

`Bus` is bound to one connection and holds two routers — one per direction:

```
publish:   Message ──▶ Bus::publish()  ──▶ PublisherRouter ──▶ Kafka
                                            (message class → topic)

receive:   Kafka ──▶ Worker (poll) ──▶ Bus::dispatch() ──▶ ReceiverRouter ──▶ Handler
                                                            (topic → handler)
```

Publishing and receiving usually run in different OS processes — a web request publishes, a long-running worker receives — but both go through the same `Bus` configuration.

Reading from Kafka is deliberately not `Bus`'s job. A worker only knows which topics to poll; it reads raw messages and calls `$bus->dispatch()`. It has no knowledge of handlers, routing, or middleware. This means:

- a message can be dispatched to `Bus` from any source — a worker, a test, an Outbox relay;
- routes and middleware are declared once, next to the publisher routes;
- the polling loop can be swapped without touching business code.

## Outgoing message path

```
$bus->publish($message)
  │  wraps it in MessageBatch::fromArray([$message])
  ▼
Publisher → PublisherRouter
  │  finds the Route by message class
  ▼
PublisherStream
  │  for each message:
  │  PublisherPipelineHandler + route middleware
  │  → ProducerMessage (topic, payload, headers, key, partition)
  ▼
Producer (rdkafka) → producev() → poll() → flush()
```

`PublisherRouter` caches the `PublisherStream` per message class, so a producer for a topic is created only once.

## Incoming message path

```
WorkerRunner::run()
  │  ConsumerStream: subscribe(topics), then loop:
  │  consumer->getMessage()
  ▼
$bus->dispatch($message)
  ▼
Receiver → ReceiverRouter
  │  finds the Route by physical topic name
  ▼
RouteExecutor
  │  MessageFactory: ConsumerMessage → handler argument
  │  ReceiverPipelineHandler + route middleware
  ▼
Handler (your business logic)
  │
  ▼ back in ConsumerStream: consumer->commit()
```

`ReceiverRouter` caches the `RouteExecutor` per topic. The commit happens in the worker, after `dispatch()` returns successfully.

## Routes and lazy resolution

Both routers extend `Routing\AbstractRouter` and read routes from a `Routing\RouteCollection`:

- the publisher collection is keyed by message class;
- the receiver collection is keyed by physical topic name;
- adding a route with an existing key replaces the old one.

A collection can hold a `Routing\LazyRoute` instead of a ready route. A lazy route stores its key and a factory closure; the route is built on the first `get()` and cached. The Laravel bridge uses this to resolve handlers and middleware from the container only when the first message for a topic arrives.

```php
use KafkaBus\Core\Receivers\Routing\Route;
use KafkaBus\Core\Routing\LazyRoute;
use KafkaBus\Core\Routing\RouteCollection;

$routes = (new RouteCollection(Route::class))
    ->add(new LazyRoute(
        'production.fact.products.1',
        fn (): Route => new Route($topicRegistry->get('products'), new ProductHandler()),
    ));
```

::: warning
The key of a `LazyRoute` must match the key of the route its factory builds — the topic name for receiver routes, the message class for publisher routes. On a mismatch, `resolve()` throws a `LogicException`.
:::

## Pipeline and middleware

Middleware can be attached at two places:

| Direction | Configured in | Interface | `target()` | Extra |
|---|---|---|---|---|
| Publish | `Publishers\Routing\Options::$middleware` | `PublisherPipelineMiddleware` | `ProducerMessageInterface` | `topic()`, `withHeader()` |
| Receive | `Receivers\Routing\RouteInfo::$middleware` | `ReceiverPipelineMiddleware` | `ConsumerMessageInterface` | `formatted()` |

### How the pipeline works

Each message passes through the middleware chain in order. Each middleware receives a `PipelineInterface` and explicitly passes control onward by calling `continue()`:

```
message → Middleware1 → Middleware2 → Middleware3 → Handler
                                                        ↓
          Middleware1 ← Middleware2 ← Middleware3 ← response
```

The mechanics live in `Pipeline`:

- `continue()` takes the next middleware off the queue and calls it; once the queue is empty, it runs `handler()->handle()` and stores the result;
- `start()` kicks off the chain and returns `result()`;
- calling `continue()` again after completion throws a `LogicException`.

```php
public function handle(PipelineInterface $pipeline): PipelineInterface
{
    // before processing
    $pipeline->continue();
    // after processing

    return $pipeline;
}
```

::: warning Breaking the chain
If a middleware returns `$pipeline` without calling `continue()`, the handler never runs and `result()` stays `null`.

When receiving, this means "message skipped": the worker still commits the offset. `ConsumerCommiterMiddleware` uses exactly this to skip duplicates. When publishing, the message isn't sent: `PublisherStream` skips `null` results.
:::

## Error resilience

### Retries

`Utils\RetryRepeater` retries an operation on exception: up to `maximumRetries` attempts with a `sleepAfterError` microsecond pause between them (defaults are `10` attempts and `1000` µs). It's used in two places:

- `Producer` — retries `flush()` until delivery is confirmed;
- `Consumer::commit()` — retries the offset commit.

### Ignorable consumer errors

The worker's `ConsumerStream` doesn't stop on routine "empty" broker responses and keeps looping on these codes:

- `RD_KAFKA_RESP_ERR__PARTITION_EOF` — end of partition reached;
- `RD_KAFKA_RESP_ERR__TRANSPORT` — transient network error;
- `RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT`;
- `RD_KAFKA_RESP_ERR__TIMED_OUT` — `consumerTimeout` expired.

Any other error code is rethrown as a `MessageConsumerException`.

### Handler errors

`Receiver::dispatch()` wraps any exception from routing, the message factory, middleware, or the handler in a `MessageConsumerNotHandledException`. The original exception is available via `getPrevious()`, the failed message via the public `$consumerMessage` property. The worker doesn't catch it: the exception leaves `WorkerRunner::run()` and the offset of that message isn't committed.

### Exception hierarchy

```
KafkaBus\Core\Exceptions\
├── Connections\
│   ├── ConnectionException                 # connection not found
│   └── DriverException                     # no driver registered for the config
├── Consumers\
│   ├── ConsumerException                   # read error / null connection
│   ├── MessageConsumerException            # message arrived with an error code
│   ├── MessageConsumerNotHandledException  # dispatch failed, wraps the cause
│   └── RouteConsumerException              # no route for the topic
├── Producers\
│   ├── ProducerException
│   ├── CannotFlushProducerException        # flush not confirmed
│   └── RouteProducerException              # no route for the message class
└── TopicCannotResolvedException            # topic key not registered

KafkaBus\Worker\Exceptions\
└── WorkerException                         # worker not found in the registry

KafkaBus\Metadata\Exceptions\
├── CannotCommitOffsetException             # offset can't be set
└── TopicNotFoundException                  # topic missing from the cluster
```

## Directory structure

```
packages/
├── core/src/
│   ├── Bus.php, BusInterface.php
│   ├── Connections/   # KafkaConnection, NullConnection, ConnectionRegistry, DriverRegistry, Config
│   ├── Consumers/     # Consumer, ConsumerConfig, Messages, Commiters
│   ├── Producers/     # Producer, ProducerConfig, Messages (HasKey, HasHeaders, HasPartition)
│   ├── Publishers/    # Publisher, PublisherStream, MessageBatch, Routing, Pipelines
│   ├── Receivers/     # Receiver, LazyReceiver, Routing, Messages, Attributes, Pipelines
│   ├── Routing/       # AbstractRouter, RouteCollection, LazyRoute
│   ├── Pipelines/     # Pipeline, PipelineBuilder
│   ├── Topics/        # Topic, TopicRegistry
│   ├── Testing/       # BusFaker, ConnectionFaker, MessageFactory, assertion drivers
│   ├── Exceptions/
│   └── Utils/         # RetryRepeater
├── worker/src/        # Worker, WorkerRunner, WorkerRunnerFactory, WorkerMerger, ConsumerStream
├── metadata/src/      # Metadata, TopicsMetadata, ConsumerGroupMetadata, Partitions
├── messages/src/      # Payload, DomainMessage, JsonMessage, casters, factories
└── commiter/src/      # ConsumerCommiterMiddleware, PublisherIdempotencyMiddleware, repositories
```
