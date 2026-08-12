# Pipeline

При обработке сообщения запускаются два независимых pipeline:

| Pipeline     | Где настраивается                    | Интерфейс middleware         | Что в `target()`               |
|--------------|--------------------------------------|------------------------------|--------------------------------|
| **Consumer** | `Workers\Options::$middleware`       | `ConsumerPipelineMiddleware` | `ConsumerMessageInterface`     |
| **Route**    | `RouteInfo::$middleware`             | `MessagePipelineMiddleware`  | Результат `MessageFactory`     |

Consumer pipeline общий для всех топиков воркера и видит сырое сообщение Kafka. Route pipeline настраивается для каждого топика отдельно и видит уже десериализованный объект.

```
ConsumerMessage
    │
    ▼ [Consumer middleware воркера]
ConsumerRouter → RouteExecutor
    │ MessageFactory: ConsumerMessage → OrderMessage
    ▼ [Route middleware маршрута]
Обработчик
```

## Middleware консьюмера

### Интерфейс

```php
use KafkaBus\Core\Consumers\Pipelines\ConsumerPipelineHandler;
use KafkaBus\Core\Consumers\Pipelines\ConsumerPipelineMiddleware;
use KafkaBus\Core\Interfaces\Consumers\Messages\ConsumerMessageInterface;
use KafkaBus\Core\Interfaces\Pipelines\PipelineInterface;

final class LoggingMiddleware implements ConsumerPipelineMiddleware
{
    /**
     * @param PipelineInterface<ConsumerMessageInterface, ConsumerPipelineHandler> $pipeline
     * @return PipelineInterface<ConsumerMessageInterface, ConsumerPipelineHandler>
     */
    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        $message = $pipeline->handler()->target();
        $start   = hrtime(true);

        $pipeline->continue();

        $elapsed = (hrtime(true) - $start) / 1e6;

        echo "Обработано за {$elapsed}мс: {$message->topicName()}" . PHP_EOL;

        return $pipeline;
    }
}
```

`PipelineInterface` параметризован двумя типами: результатом и классом обработчика.

### Middleware с обработкой ошибок

По умолчанию исключение из обработчика останавливает воркер. Чтобы этого не происходило, перехватите его в middleware:

```php
final class ErrorHandlingMiddleware implements ConsumerPipelineMiddleware
{
    public function __construct(private readonly LoggerInterface $logger) {}

    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        $message = $pipeline->handler()->target();

        try {
            return $pipeline->continue();
        }
        catch (\Throwable $e) {
            $this->logger->error('Ошибка обработки сообщения', [
                'topic'     => $message->topicName(),
                'partition' => $message->original()->partition,
                'offset'    => $message->original()->offset,
                'payload'   => $message->payload(),
                'exception' => $e,
            ]);

            return $pipeline;
        }
    }
}
```

::: warning
Поглощая исключение, вы соглашаетесь с тем, что сообщение будет **закоммичено как обработанное** и больше не вернётся. Цикл `ConsumerStream` вызывает `commit()` сразу после `handle()`, если тот не выбросил исключение.

Если сообщение нужно обработать повторно — не глушите исключение здесь, а используйте [`ConsumerCommiterMiddleware`](/ru/docs/components/commiter) со счётчиком попыток или отправляйте payload в DLQ-топик перед возвратом pipeline.
:::

::: tip
Исключения из обработчика доходят до middleware уже обёрнутыми в `MessageConsumerNotHandledException`. Оригинал доступен через `$e->getPrevious()`.
:::

### Прерывание обработки

Если middleware вернёт pipeline **без** `continue()`, обработчик не выполнится, но сообщение всё равно будет закоммичено — с точки зрения `ConsumerStream` итерация завершилась успешно. Так работает пропуск дублей в `ConsumerCommiterMiddleware`.

```php
final class SkipTombstonesMiddleware implements ConsumerPipelineMiddleware
{
    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        if ($pipeline->handler()->target()->payload() === '') {
            return $pipeline; // tombstone — пропускаем, но offset двигаем
        }

        return $pipeline->continue();
    }
}
```

### Подключение

Middleware применяется ко всем топикам воркера:

```php
use KafkaBus\Core\Bus\Listeners\Workers\MemoryWorkerRegistry;
use KafkaBus\Core\Bus\Listeners\Workers\Options;
use KafkaBus\Core\Bus\Listeners\Workers\Worker;

$workerRegistry = MemoryWorkerRegistry::make()
    ->add(new Worker(
        name:    'default',
        routes:  $consumerRoutes,
        options: new Options(
            middleware: [
                new ErrorHandlingMiddleware($logger),
                new LoggingMiddleware(),
            ]
        )
    ));
```

## Middleware маршрута

Route middleware видит объект, который получит обработчик. Предположим, есть обработчик заказов:

```php
class OrderHandler
{
    #[MessageFactory(new DomainMessageFactory(OrderMessage::class))]
    public function __invoke(OrderMessage $order): void
    {
        // Логика обработки заказа
    }
}
```

### Интерфейс

```php
use KafkaBus\Core\Consumers\Pipelines\MessagePipelineHandler;
use KafkaBus\Core\Consumers\Pipelines\MessagePipelineMiddleware;
use KafkaBus\Core\Interfaces\Pipelines\PipelineInterface;

final class CheckingMiddleware implements MessagePipelineMiddleware
{
    /**
     * @param PipelineInterface<mixed, MessagePipelineHandler> $pipeline
     * @return PipelineInterface<mixed, MessagePipelineHandler>
     */
    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        $order = $pipeline->handler()->target(); // OrderMessage

        if ($order->getEvent() === DomainEventEnum::Delete) {
            return $pipeline; // не передаём в обработчик
        }

        return $pipeline->continue();
    }
}
```

::: tip
`target()` возвращает `mixed` — конкретный тип зависит от `MessageFactory` маршрута. Если middleware пишется под конкретный топик, тип известен; если middleware переиспользуется, проверяйте его через `instanceof`.
:::

### Подключение

Middleware применяется только к конкретному топику:

```php
use KafkaBus\Core\Consumers\Router\ConsumerRoutesBuilder;
use KafkaBus\Core\Consumers\Router\RouteInfo;

$consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry)
    ->add(new RouteInfo(
        topicKey:   'orders',
        handler:    new OrderHandler(),
        middleware: [new CheckingMiddleware()],
    ))
    ->build();
```

## Порядок выполнения

Middleware выполняются в порядке добавления, сначала весь Consumer pipeline, затем Route pipeline:

```
[middleware воркера]  →  [middleware маршрута]  →  [обработчик]
ErrorHandling         →  Checking               →  OrderHandler
LoggingMiddleware
```

Как и у продюсера, код до `continue()` выполняется на «пути туда», после — на «пути обратно». Экземпляры middleware переиспользуются между сообщениями — не храните в них состояние конкретного сообщения.

## Выбор уровня

| Задача                                        | Уровень   |
|-----------------------------------------------|-----------|
| Логирование, метрики, трейсинг                | Consumer  |
| Обработка ошибок, DLQ                         | Consumer  |
| Идемпотентность (`ConsumerCommiterMiddleware`)| Consumer  |
| Доступ к сырому payload и заголовкам          | Consumer  |
| Валидация десериализованного объекта          | Route     |
| Фильтрация по бизнес-полям                    | Route     |
| Мультитенантность по данным сообщения         | Route     |

## Встроенные middleware из экосистемы

| Middleware                   | Пакет                | Описание                          |
|------------------------------|----------------------|-----------------------------------|
| `ConsumerCommiterMiddleware` | `kafka-bus/commiter` | Идемпотентность, защита от дублей |

::: tip
Подробнее о `ConsumerCommiterMiddleware` — в разделе [Commiter](/ru/docs/components/commiter).
:::
