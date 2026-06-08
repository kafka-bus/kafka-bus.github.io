# Configure

## Поток потребления

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
  Consumer Pipeline
    │
    ▼
  ConsumerRouter → Route Pipeline → MessageHandler (бизнес-логика)
```

Подробно:

1. Приложение запрашивает слушателя: `Bus::listener('worker-name')`.
2. `Bus` делегирует в `Thread`, который через `ListenerFactory` создаёт `Listener`.
3. `Listener` запускает `ConsumerStream` — блокирующий цикл чтения.
4. Каждое сообщение проходит через Consumer Pipeline.
5. `ConsumerRouter` находит нужный `MessageHandler` по имени топика.
6. Сообщение конвертируется через `MessageFactory`.
7. Передает сообщение в Route Pipeline.
8. Обработчик выполняет бизнес-логику.

## Маршруты

`ConsumerRoutesBuilder` связывает логический ключ топика с обработчиком:

```php
use KafkaBus\Core\Consumers\Router\ConsumerRoutesBuilder;
use KafkaBus\Core\Consumers\Router\RouteInfo;

$consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry)
    ->add(new RouteInfo('products', new ProductHandler()))
    ->add(new RouteInfo('orders',   new OrderHandler()))
    ->build();
```

### Маршрут с middleware

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

## Воркеры

Воркер — именованная конфигурация consumer'а. Один воркер может слушать несколько топиков:

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

## Опции consumer

| Опция librdkafka        | Описание                  | Рекомендуемое значение  |
|-------------------------|---------------------------|-------------------------|
| `group.id`              | Имя consumer group        | Имя вашего сервиса      |
| `auto.offset.reset`     | Позиция при первом старте | `earliest` или `latest` |
| `max.poll.interval.ms`  | Макс. время между poll    | `300000`                |
| `session.timeout.ms`    | Таймаут сессии            | `45000`                 |
| `heartbeat.interval.ms` | Интервал heartbeat        | `3000`                  |
| `enable.auto.commit`    | Авто-коммит offset        | `false` (рекомендуется) |