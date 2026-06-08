# Архитектура

## Ключевые концепции

| Концепция      | Класс                   | Описание                                                             |
|----------------|-------------------------|----------------------------------------------------------------------|
| **Bus**        | `Micromus\KafkaBus\Bus` | Центральный фасад. Единая точка входа для публикации и прослушивания |
| **Connection** | `ConnectionInterface`   | Настроенное подключение к кластеру Kafka                             |
| **Thread**     | `ThreadInterface`       | Логическая обёртка над соединением, содержит Publisher и Listener    |
| **Publisher**  | `PublisherInterface`    | Высокоуровневая абстракция для отправки сообщений                    |
| **Producer**   | `ProducerInterface`     | Низкоуровневый компонент, взаимодействующий с rdkafka                |
| **Listener**   | `ListenerInterface`     | Высокоуровневая абстракция для прослушивания топиков                 |
| **Consumer**   | `ConsumerInterface`     | Низкоуровневый компонент чтения из rdkafka                           |
| **Topic**      | `Topic`                 | Именованный канал в Kafka с логическим ключом                        |
| **Pipeline**   | `PipelineInterface`     | Middleware-конвейер для входящих и исходящих сообщений               |

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


## Pipeline (Middleware)

Пакет реализует паттерн Middleware через конвейер (`Pipeline`). Middleware можно добавлять на трёх уровнях: глобально для воркера, для конкретного маршрута топика, и для маршрута producer'а.

## Как работает конвейер

Каждое входящее или исходящее сообщение проходит через цепочку middleware по порядку. Каждый middleware получает сообщение `PipelineInterface`, который передаёт управление следующему звену:

```
сообщение → Middleware1 → Middleware2 → Middleware3 → Handler
                                                          ↓
           Middleware1 ← Middleware2 ← Middleware3 ← ответ
```
