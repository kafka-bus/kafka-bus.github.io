# Настройка консьюмера

## Поток обработки

```
Приложение
    │
    ▼ listener(workerName)
  Bus → Thread → ListenerFactory → WorkerRegistry
    │
    ▼ listen()
  Listener → ConsumerStream (блокирующий цикл)
    │
    ▼ getMessage()
  Consumer → Kafka Broker
    │ ← ConsumerMessage
    ▼
  MessageHandler
    │ → Consumer Pipeline (middleware воркера)
    ▼
  ConsumerRouter → RouteExecutor
    │ → MessageFactory (десериализация)
    │ → Route Pipeline (middleware маршрута)
    ▼
  Обработчик (бизнес-логика)
    │
    ▼ commit()
```

Подробнее:

1. Приложение запрашивает listener: `Bus::listener('worker-name')`.
2. `ListenerFactory` достаёт `Worker` из `WorkerRegistryInterface` и собирает `ConsumerStream`.
3. `ConsumerStream` запускает блокирующий цикл чтения.
4. Каждое сообщение проходит через Consumer Pipeline воркера.
5. `ConsumerRouter` находит маршрут по **физическому имени топика**.
6. `MessageFactory` конвертирует сообщение в тип, ожидаемый обработчиком.
7. Сообщение проходит Route Pipeline и попадает в обработчик.
8. При успехе выполняется `commit()`.

## Маршруты

`ConsumerRoutesBuilder` маппит логический ключ топика на обработчик:

```php
use KafkaBus\Core\Consumers\Router\ConsumerRoutesBuilder;
use KafkaBus\Core\Consumers\Router\RouteInfo;

$consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', new ProductHandler()))
    ->add(new RouteInfo('orders',   new OrderHandler()))
    ->build();
```

### Параметры маршрута

```php
new RouteInfo(
    topicKey:   'orders',            // логический ключ топика
    handler:    new OrderHandler(),  // любой callable
    middleware: [],                  // list<MessagePipelineMiddleware>
);
```

| Параметр     | Тип                               | По умолчанию | Описание                       |
|--------------|-----------------------------------|--------------|--------------------------------|
| `topicKey`   | `string`                          | —            | Ключ топика из `TopicRegistry` |
| `handler`    | `callable`                        | —            | Обработчик сообщения           |
| `middleware` | `list<MessagePipelineMiddleware>` | `[]`         | Middleware уровня маршрута     |

### Маршрут с middleware

```php
$consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry)
    ->add(new RouteInfo(
        topicKey:   'orders',
        handler:    new OrderHandler(),
        middleware: [new TenantMiddleware()],
    ))
    ->build();
```

### Фабрика сообщений по умолчанию

Вторым аргументом `ConsumerRoutesBuilder::make()` можно задать фабрику, которая применится ко всем маршрутам, где тип аргумента обработчика распознать не удалось:

```php
use KafkaBus\Core\Consumers\Messages\StringMessageFactory;

$consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry, new StringMessageFactory())
    ->add(new RouteInfo('products', $legacyHandler))
    ->build();
```

Без второго аргумента используется `NativeMessageFactory`. Приоритет разрешения описан в разделе [Получение сообщений](/ru/docs/1.x/consumer/consuming#порядок-разрешения-фабрики).

## Воркеры

Воркер — именованная конфигурация консьюмера. Один воркер может слушать несколько топиков:

```php
use KafkaBus\Core\Bus\Listeners\Workers\MemoryWorkerRegistry;
use KafkaBus\Core\Bus\Listeners\Workers\Options;
use KafkaBus\Core\Bus\Listeners\Workers\Worker;

$workerRegistry = MemoryWorkerRegistry::make()
    ->add(new Worker(
        name:    'default',
        routes:  $consumerRoutes,
        options: new Options(
            additionalOptions: [
                'group.id'              => 'my-service',
                'auto.offset.reset'     => 'earliest',
                'max.poll.interval.ms'  => 300_000,
                'session.timeout.ms'    => 45_000,
                'heartbeat.interval.ms' => 3_000,
            ],
            middleware:      [],
            autoCommit:      false,
            consumerTimeout: 5_000,
        )
    ));
```

Реестр передаётся в `ListenerFactory`:

```php
use KafkaBus\Core\Bus\Listeners\ListenerFactory;
use KafkaBus\Core\Consumers\ConsumerStreamFactory;

$listenerFactory = new ListenerFactory(
    new ConsumerStreamFactory(),
    $workerRegistry,
);
```

Обращение к незарегистрированному имени воркера бросает `ListenerException`.

### Параметры воркера

| Параметр            | Тип                                | По умолчанию | Описание                                          |
|---------------------|------------------------------------|--------------|---------------------------------------------------|
| `additionalOptions` | `array<string, scalar\|null>`      | `[]`         | Параметры librdkafka для консьюмера               |
| `middleware`        | `list<ConsumerPipelineMiddleware>` | `[]`         | Middleware уровня воркера (все топики)            |
| `autoCommit`        | `bool`                             | `true`       | Режим коммита оффсетов, см. [Коммит оффсетов](#коммит-оффсетов) |
| `consumerTimeout`   | `int`                              | `2000`       | Таймаут ожидания сообщения в `consume()`, мс      |

::: warning Фильтрация additionalOptions
Ключи фильтруются по белому списку `Options::CONSUMER_OPTIONS` — параметры продюсера (например, `compression.codec`) молча отбрасываются. Полный список — в разделе [Конфигурация](/ru/docs/1.x/configuration#фильтрация-параметров-продюсера-и-консьюмера).
:::

## Коммит оффсетов

Флаг `autoCommit` определяет, кто коммитит оффсеты группы консьюмеров. Он же задаёт параметр librdkafka `enable.auto.commit` — значение из `additionalOptions` перезаписывается.

| `autoCommit` | `enable.auto.commit` | Класс | Поведение |
|---|---|---|---|
| `true` (по умолчанию) | `'true'` | `VoidCommiter` | librdkafka коммитит оффсеты в фоне по таймеру; kafka-bus сам не коммитит |
| `false` | `'false'` | `DefaultCommiter` | Вызывает `KafkaConsumer::commitAsync()` после каждого успешно обработанного сообщения |

- **`autoCommit: false`** — коммит строго после успешной обработки каждого сообщения. Предсказуемо, at-least-once, рекомендуется, когда потеря сообщения недопустима.
- **`autoCommit: true`** — offset двигается по таймеру независимо от результата обработки. Меньше запросов к брокеру, но при падении процесса прочитанные, но не обработанные сообщения могут быть пропущены.

::: warning Исправлено в 1.3.1
До `kafka-bus/core` 1.3.1 коммитеры были перепутаны: при `autoCommit: true` коммит шёл после каждого сообщения, а при `autoCommit: false` не коммитилось ничего. Если вы рассчитывали на старое поведение `true`, после обновления переключитесь на `autoCommit: false`.
:::

Коммит выполняется через `RetryRepeater`: до 10 попыток с паузой 1000 мкс.

## Устойчивость цикла

`ConsumerStream` игнорирует штатные «пустые» ответы брокера и продолжает работу:

| Код ошибки                            | Причина                            |
|---------------------------------------|------------------------------------|
| `RD_KAFKA_RESP_ERR__PARTITION_EOF`    | Достигнут конец партиции           |
| `RD_KAFKA_RESP_ERR__TRANSPORT`        | Временная сетевая ошибка           |
| `RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT` | Брокер не ответил вовремя          |
| `RD_KAFKA_RESP_ERR__TIMED_OUT`        | Истёк `consumerTimeout`            |

Остальные коды пробрасываются как `MessageConsumerException` и останавливают воркер.

::: tip
`RD_KAFKA_RESP_ERR__TIMED_OUT` возникает штатно каждые `consumerTimeout` мс, когда в топике нет новых сообщений. Это не ошибка — просто цикл делает следующую итерацию.
:::

## Параметры консьюмера

| Параметр librdkafka     | Описание                         | Рекомендуемое значение  |
|-------------------------|----------------------------------|-------------------------|
| `group.id`              | Имя группы потребителей          | Имя вашего сервиса      |
| `auto.offset.reset`     | Позиция при первом запуске       | `earliest` или `latest` |
| `max.poll.interval.ms`  | Макс. время между опросами       | `300000`                |
| `session.timeout.ms`    | Таймаут сессии                   | `45000`                 |
| `heartbeat.interval.ms` | Интервал heartbeat               | `3000`                  |

::: tip
`enable.auto.commit` в таблице нет: он всегда выводится из `autoCommit`, см. [Коммит оффсетов](#коммит-оффсетов).
:::

## Просмотр и сброс оффсетов

`Listener::partitions()` даёт доступ к текущим offset воркера и позволяет их переустановить:

```php
use KafkaBus\Core\Bus\Listeners\Partitions\CommitOffset;
use KafkaBus\Core\Bus\Listeners\Partitions\Offset;

$partitions = $bus->listener('products')->partitions();

foreach ($partitions->list() as $partition) {
    echo "{$partition->topic->key} #{$partition->id}: "
       . "{$partition->currentOffset} / [{$partition->minOffset}, {$partition->maxOffset}]\n";
}

// Перечитать топик с начала во всех партициях
$partitions->setOffset(new CommitOffset(
    topic:     $topicRegistry->get('products'),
    partition: RD_KAFKA_PARTITION_UA, // все партиции
    offset:    Offset::Early,         // либо Offset::Latest, либо конкретное число
));
```

`setOffset()` бросает `CannotCommitOffsetException`, если топик не принадлежит воркеру, партиция не найдена или offset выходит за границы `[minOffset, maxOffset]`.

::: warning
Метод `partitions()` доступен только для подключений, реализующих `ConnectionHasTopicsInterface` (то есть `KafkaConnection`). Для `NullConnection` он бросит `LogicException`.

Воркер должен быть остановлен: запущенный процесс перезапишет позицию при следующем коммите.
:::

В Laravel эти операции доступны как команды `kafka:offset:show` и `kafka:offset:set` — см. [Команды](/ru/docs/1.x/laravel/commands).
