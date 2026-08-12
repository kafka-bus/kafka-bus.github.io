# Architecture

## Key Concepts

| Concept        | Class / interface              | Description                                                        |
|----------------|---------------------------------|---------------------------------------------------------------------|
| **Bus**        | `KafkaBus\Core\Bus`            | Central facade. The single entry point for publishing and listening |
| **Connection** | `ConnectionInterface`          | A configured connection to a Kafka cluster                          |
| **Thread**     | `ThreadInterface`              | A logical wrapper over a connection, holds the Publisher and Listener |
| **Publisher**  | `Bus\Publishers\Publisher`     | High-level abstraction for sending messages                         |
| **Producer**   | `ProducerInterface`            | Low-level component that talks to rdkafka                           |
| **Listener**   | `ListenerInterface`            | High-level abstraction for listening to topics                      |
| **Consumer**   | `ConsumerInterface`            | Low-level component that reads via rdkafka                          |
| **Worker**     | `Bus\Listeners\Workers\Worker` | A named set of consumer routes and its settings                     |
| **Topic**      | `Topics\Topic`                 | A named channel in Kafka with a logical key                         |
| **Pipeline**   | `PipelineInterface`            | Middleware pipeline for incoming and outgoing messages               |

## Directory Structure

```
src/
├── Bus/              # Bus facade, Thread, ThreadRegistry, ThreadFactory, MessageBatch
│   ├── Listeners/    # ListenerFactory, Workers, Partitions (offset operations)
│   └── Publishers/   # PublisherFactory, Router (PublisherRoutesBuilder, Options)
├── Connections/      # KafkaConnection, NullConnection, ConnectionRegistry, DriverRegistry
├── Consumers/        # ConsumerStream, Router, Handlers, Messages, Commiters, Attributes
├── Producers/        # Producer, ProducerStream, Messages, Pipelines
├── Interfaces/       # Contracts for all key components
├── Pipelines/        # Pipeline and PipelineBuilder
├── Topics/           # Topic, TopicRegistry
├── Testing/          # ConnectionFaker, ProducerFaker, ConsumerFaker, MessageFactory
├── Exceptions/       # Exception hierarchy
└── Support/          # RetryRepeater
```

## Connections and Threads

`Bus` supports any number of named connections. Each connection lives in its own `Thread`:

```
Bus
├── Thread[default]    ← ConnectionRegistry['kafka-main']
│   ├── Publisher
│   └── Listener
└── Thread[analytics]  ← ConnectionRegistry['kafka-analytics']
    ├── Publisher
    └── Listener
```

`Thread` is not an OS-level thread — it's a logical wrapper over a connection. `ThreadRegistry` creates them lazily and caches them by connection name, so a repeated `onConnection('analytics')` returns the same object.

An important consequence: `Publisher` is created right away in the `Thread` constructor, while `Listener` is only created when `listener()` is called. The producer route table `PublisherRoutes`, however, is shared across all connections — it's passed into `PublisherFactory` once.

## Outgoing Message Path

```
$bus->publish($message)
  │
  ▼ Bus → Thread → Publisher → PublisherRouter
  │        finds the Route by message class
  ▼
ProducerStream
  │  for each message:
  │  ProducerPipelineHandler + route middleware
  │  → ProducerMessage (payload, headers, key, partition)
  ▼
Producer (rdkafka) → produce() → poll() → flush()
```

`PublisherRouter` caches `ProducerStream` by message class, so the producer and its connection are created only once per class.

## Incoming Message Path

```
$bus->listener('worker')->listen()
  │
  ▼ ListenerFactory → Worker from WorkerRegistry
  ▼ ConsumerStreamFactory → ConsumerStream + MessageHandler
  │
  ▼ infinite loop:
ConsumerStream::listen()
  │  consumer->getMessage()
  ▼
MessageHandler  ─── Consumer Pipeline (worker middleware)
  │                  target: ConsumerMessageInterface
  ▼
ConsumerRouter  ─── finds the Route by topic name
  │
  ▼ MessageFactory: ConsumerMessage → handler argument type
  │
  ▼ Route Pipeline (route middleware)
  ▼
Handler (your business logic)
  │
  ▼ consumer->commit()
```

The two pipelines aren't duplication: the first sees the raw Kafka message and is shared across all of the worker's topics; the second sees the already-deserialized object and is configured per topic.

## Pipeline and Middleware

Middleware can be attached at three levels:

| Level             | Configured in                            | Interface                     | What `target()` sees        |
|--------------------|-------------------------------------------|--------------------------------|-------------------------------|
| Worker             | `Workers\Options::$middleware`           | `ConsumerPipelineMiddleware`  | `ConsumerMessageInterface`   |
| Consumer route     | `RouteInfo::$middleware`                 | `MessagePipelineMiddleware`   | the `MessageFactory` result   |
| Producer route     | `Publishers\Router\Options::$middleware` | `ProducerPipelineMiddleware`  | `ProducerMessageInterface`   |

### How the Pipeline Works

Each message passes through the middleware chain in order. Each middleware receives a `PipelineInterface` and explicitly passes control onward by calling `continue()`:

```
message → Middleware1 → Middleware2 → Middleware3 → Handler
                                                        ↓
          Middleware1 ← Middleware2 ← Middleware3 ← response
```

The mechanics live in `Pipeline`:

- `continue()` pops the first middleware off the queue and calls it; once the queue is empty, it runs `handler()->handle()` and stores the result;
- `start()` kicks off the chain and returns `result()`;
- calling `continue()` again after completion throws a `LogicException`.

```php
public function handle(PipelineInterface $pipeline): PipelineInterface
{
    // before processing
    $pipeline->continue();   // pass control onward
    // after processing

    return $pipeline;
}
```

::: warning Breaking the Chain
If a middleware returns `$pipeline` **without** calling `continue()`, the handler never runs and `result()` stays `null`.

For a consumer this means "message skipped" (this is exactly how `ConsumerCommiterMiddleware` handles duplicates). For a producer, the message **won't be sent**: `ProducerStream` skips `null` results.
:::

## Error Resilience

### RetryRepeater

`Support\RetryRepeater` recursively retries an operation on exception: up to `maximumRetries` attempts with a `sleepAfterError` microsecond pause between them (defaults are `10` attempts and `1000` µs). It's used in two places:

- `Producer::flush()` — retries confirming delivery;
- `Consumer::commit()` — retries the offset commit.

### Ignorable Consumer Errors

`ConsumerStream` doesn't crash the worker on routine "empty" broker responses and keeps looping on the following codes:

- `RD_KAFKA_RESP_ERR__PARTITION_EOF` — end of partition reached;
- `RD_KAFKA_RESP_ERR__TRANSPORT` — transient network error;
- `RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT`;
- `RD_KAFKA_RESP_ERR__TIMED_OUT` — `consumerTimeout` expired.

Any other error code is rethrown as a `MessageConsumerException`.

### Exception Hierarchy

```
Exceptions/
├── Connections/
│   ├── ConnectionException            # connection not found
│   └── DriverException                # no driver registered for the config
├── Consumers/
│   ├── ConsumerException              # read error / null connection
│   ├── MessageConsumerException       # message arrived with an error code
│   ├── MessageConsumerNotHandledException  # handler threw an exception
│   └── RouteConsumerException         # no route for the topic
├── Producers/
│   ├── ProducerException
│   ├── CannotFlushProducerException   # flush not confirmed
│   └── RouteProducerException         # no route for the message class
├── Listeners/
│   ├── ListenerException              # worker not found
│   └── CannotCommitOffsetException    # offset out of valid range
├── TopicCannotResolvedException       # topic key not registered
└── TopicNotFoundException             # topic missing from the cluster
```

Any exception thrown from a handler is wrapped in a `MessageConsumerNotHandledException`, with the original exception available via `previous`.

## Multiple Workers in One Process

`Bus::listener()` accepts either a worker name or an array of names:

```php
$bus->listener('products')->listen();
$bus->listener(['products', 'orders'])->listen();
```

When given an array, `WorkerMerger` combines the workers into a single synthetic `grouped-worker`: the routes from all workers are merged into one table, and each route's handler becomes a `RouteHandler` that runs the *original* worker's own `MessageHandler`. This preserves each worker's individual middleware set.

::: warning
When merging, connection settings (`additionalOptions`, `autoCommit`, `consumerTimeout`) are taken **only from the first worker** in the list. The rest are ignored, so it only makes sense to merge workers that share the same `group.id` and commit mode.
:::
