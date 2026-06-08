# Architecture

## Key Concepts

| Concept        | Class                   | Description                                                              |
|----------------|-------------------------|--------------------------------------------------------------------------|
| **Bus**        | `Micromus\KafkaBus\Bus` | Central facade. Single entry point for publishing and listening           |
| **Connection** | `ConnectionInterface`   | A configured connection to a Kafka cluster                               |
| **Thread**     | `ThreadInterface`       | Logical wrapper over a connection, contains Publisher and Listener       |
| **Publisher**  | `PublisherInterface`    | High-level abstraction for sending messages                              |
| **Producer**   | `ProducerInterface`     | Low-level component interacting with rdkafka                             |
| **Listener**   | `ListenerInterface`     | High-level abstraction for listening to topics                           |
| **Consumer**   | `ConsumerInterface`     | Low-level rdkafka read component                                         |
| **Topic**      | `Topic`                 | A named channel in Kafka with a logical key                              |
| **Pipeline**   | `PipelineInterface`     | Middleware pipeline for incoming and outgoing messages                   |

## Directory Structure

```
src/
├── Bus/              # Bus facade, Thread, ThreadRegistry, ThreadFactory
│   ├── Listeners/    # ListenerFactory, Workers (Worker, MemoryWorkerRegistry, Options)
│   └── Publishers/   # PublisherFactory, Router (PublisherRoutesBuilder, Options)
├── Connections/      # Connections: KafkaConnection, NullConnection, ConnectionRegistry
├── Consumers/        # Reading: ConsumerStream, Router, Handlers, Attributes
├── Producers/        # Writing: ProducerStream, Messages
├── Interfaces/       # Contracts for all key components
├── Pipelines/        # Pipeline processing mechanism
├── Topics/           # Topic, TopicRegistry
├── Testing/          # ProducerFaker, ConsumerFaker, MessageFactory
├── Exceptions/       # Exception hierarchy
└── Support/          # Helper classes
```

## Connections and Threads

`Bus` supports multiple named connections. Each connection lives in its own `Thread`:

```
Bus
├── Thread[default]    ← ConnectionRegistry['kafka-main']
│   ├── Publisher
│   └── Listener
└── Thread[analytics]  ← ConnectionRegistry['kafka-analytics']
    ├── Publisher
    └── Listener
```

`Thread` is not a real OS thread — it is a logical wrapper over a connection.


## Pipeline (Middleware)

The package implements the Middleware pattern via a pipeline (`Pipeline`). Middleware can be added at three levels: globally for a worker, for a specific topic route, and for a producer route.

## How the Pipeline Works

Every incoming or outgoing message passes through the middleware chain in order. Each middleware receives a `PipelineInterface` and passes control to the next link:

```
message → Middleware1 → Middleware2 → Middleware3 → Handler
                                                        ↓
          Middleware1 ← Middleware2 ← Middleware3 ← response
```
