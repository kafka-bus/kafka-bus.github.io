# Архитектура

## Ключевые концепции

| Концепция | Класс | Описание |
|---|---|---|
| **Bus** | `Micromus\KafkaBus\Bus` | Центральный фасад. Единая точка входа для публикации и прослушивания |
| **Connection** | `ConnectionInterface` | Настроенное подключение к кластеру Kafka |
| **Thread** | `ThreadInterface` | Логическая обёртка над соединением, содержит Publisher и Listener |
| **Publisher** | `PublisherInterface` | Высокоуровневая абстракция для отправки сообщений |
| **Producer** | `ProducerInterface` | Низкоуровневый компонент, взаимодействующий с rdkafka |
| **Listener** | `ListenerInterface` | Высокоуровневая абстракция для прослушивания топиков |
| **Consumer** | `ConsumerInterface` | Низкоуровневый компонент чтения из rdkafka |
| **Topic** | `Topic` | Именованный канал в Kafka с логическим ключом |
| **Pipeline** | `PipelineInterface` | Middleware-конвейер для входящих и исходящих сообщений |

## Структура директорий

```
src/
├── Bus/              # Фасад Bus, Thread, ThreadRegistry, ThreadFactory
│   ├── Listeners/    # ListenerFactory, Workers (Worker, MemoryWorkerRegistry, Options)
│   └── Publishers/   # PublisherFactory, Router (PublisherRoutesBuilder, Options)
├── Connections/      # Соединения: KafkaConnection, NullConnection, ConnectionRegistry
├── Consumers/        # Чтение: ConsumerStream, Router, Handlers, Attributes
├── Producers/        # Запись: ProducerStream, Messages
├── Interfaces/       # Контракты для всех ключевых компонентов
├── Pipelines/        # Механизм конвейерной обработки
├── Topics/           # Topic, TopicRegistry
├── Testing/          # ProducerFaker, ConsumerFaker, MessageFactory
├── Exceptions/       # Иерархия исключений
└── Support/          # Вспомогательные классы
```

## Поток публикации сообщения

```
Приложение
    │
    ▼ publish(ProducerMessageInterface)
  Bus
    │ → ThreadRegistry → Thread
    │
    ▼
  Publisher
    │ → PublisherRouter (определяет топик)
    │ → Pipeline (middleware-цепочка)
    │
    ▼
  Producer (низкоуровневый)
    │
    ▼
  Kafka Broker
```

Подробно:

1. Приложение вызывает `Bus::publish($message)`.
2. `Bus` получает активный `Thread` через `ThreadRegistry`.
3. `Thread` может группировать сообщения в батч (`MessageBatch`) и передаёт их в `Publisher`.
4. `Publisher` через `PublisherRouter` определяет целевой топик.
5. Сообщение проходит через middleware-конвейер.
6. Низкоуровневый `Producer` отправляет сообщение через rdkafka.

## Поток потребления сообщения

```
Приложение
    │
    ▼ listener(workerName)
  Bus → Thread → ListenerFactory
    │
    ▼ listen()
  Listener
    │
    ▼
  ConsumerStream → Kafka Broker
    │ ← сообщение
    ▼
  Pipeline (middleware)
    │
    ▼
  ConsumerRouter → MessageHandler (бизнес-логика)
```

Подробно:

1. Приложение запрашивает слушателя: `Bus::listener('worker-name')`.
2. `Bus` делегирует в `Thread`, который через `ListenerFactory` создаёт `Listener`.
3. `Listener` запускает `ConsumerStream` — блокирующий цикл чтения.
4. Каждое сообщение проходит через middleware-конвейер (`Pipeline`).
5. `ConsumerRouter` находит нужный `MessageHandler` по имени топика.
6. Обработчик выполняет бизнес-логику.

## Соединения и потоки

`Bus` поддерживает несколько именованных соединений. Каждое соединение живёт в своём `Thread`:

```
Bus
├── Thread[default]    ← ConnectionRegistry['kafka-main']
│   ├── Publisher
│   └── Listener
└── Thread[analytics]  ← ConnectionRegistry['kafka-analytics']
    ├── Publisher
    └── Listener
```

`Thread` — не реальный OS-поток, а логическая обёртка над соединением.

## Тестирование

Для изоляции тестов от реальной Kafka предусмотрены фейковые реализации:

- **`ProducerFaker`** — перехватывает `Bus::publish()`, хранит сообщения в памяти, предоставляет `assertPublished()`.
- **`ConsumerFaker`** — имитирует входящие сообщения, запускает полный consumer-конвейер без реального брокера.
- **`NullConnection`** — принимает все вызовы без взаимодействия с Kafka.
