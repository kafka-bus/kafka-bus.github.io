# Архитектура

## Ключевые концепции

| Концепция      | Класс                   | Описание                                                                 |
|----------------|-------------------------|--------------------------------------------------------------------------|
| **Bus**        | `Micromus\KafkaBus\Bus` | Центральный фасад. Единая точка входа для публикации и прослушивания     |
| **Connection** | `ConnectionInterface`   | Настроенное подключение к кластеру Kafka                                 |
| **Thread**     | `ThreadInterface`       | Логическая обёртка над подключением, содержит Publisher и Listener       |
| **Publisher**  | `PublisherInterface`    | Высокоуровневая абстракция для отправки сообщений                        |
| **Producer**   | `ProducerInterface`     | Низкоуровневый компонент, взаимодействующий с rdkafka                    |
| **Listener**   | `ListenerInterface`     | Высокоуровневая абстракция для прослушивания топиков                     |
| **Consumer**   | `ConsumerInterface`     | Низкоуровневый компонент чтения через rdkafka                            |
| **Topic**      | `Topic`                 | Именованный канал в Kafka с логическим ключом                            |
| **Pipeline**   | `PipelineInterface`     | Middleware pipeline для входящих и исходящих сообщений                   |

## Структура директорий

```
src/
├── Bus/              # Фасад Bus, Thread, ThreadRegistry, ThreadFactory
│   ├── Listeners/    # ListenerFactory, Workers (Worker, MemoryWorkerRegistry, Options)
│   └── Publishers/   # PublisherFactory, Router (PublisherRoutesBuilder, Options)
├── Connections/      # Подключения: KafkaConnection, NullConnection, ConnectionRegistry
├── Consumers/        # Чтение: ConsumerStream, Router, Handlers, Attributes
├── Producers/        # Запись: ProducerStream, Messages
├── Interfaces/       # Контракты для всех ключевых компонентов
├── Pipelines/        # Механизм обработки pipeline
├── Topics/           # Topic, TopicRegistry
├── Testing/          # ProducerFaker, ConsumerFaker, MessageFactory
├── Exceptions/       # Иерархия исключений
└── Support/          # Вспомогательные классы
```

## Подключения и потоки

`Bus` поддерживает множество именованных подключений. Каждое подключение живёт в своём `Thread`:

```
Bus
├── Thread[default]    ← ConnectionRegistry['kafka-main']
│   ├── Publisher
│   └── Listener
└── Thread[analytics]  ← ConnectionRegistry['kafka-analytics']
    ├── Publisher
    └── Listener
```

`Thread` — не системный поток ОС, а логическая обёртка над подключением.

## Pipeline (Middleware)

Пакет реализует паттерн Middleware через pipeline (`Pipeline`). Middleware можно добавить на трёх уровнях: глобально для воркера, для конкретного маршрута потребителя и для маршрута продьюсера.

## Как работает Pipeline

Каждое входящее или исходящее сообщение проходит через цепочку middleware по порядку. Каждый middleware получает `PipelineInterface` и передаёт управление следующему звену:

```
message → Middleware1 → Middleware2 → Middleware3 → Handler
                                                        ↓
          Middleware1 ← Middleware2 ← Middleware3 ← response
```
