---
title: Pipeline консьюмера
description: Middleware входящих сообщений — доступ к сырому и десериализованному сообщению, обработка ошибок, пропуск сообщений и порядок выполнения
---

# Pipeline

Каждое входящее сообщение проходит pipeline своего маршрута: цепочку middleware, в конце которой вызывается обработчик. Middleware подключаются к маршруту и реализуют `ReceiverPipelineMiddleware`.

```
ConsumerMessage
    │
    ▼ MessageFactory: ConsumerMessage → аргумент обработчика
    ▼ Middleware1 → Middleware2 → ReceiverPipelineHandler::handle()
                                        │
                                        ▼
                                    Обработчик
```

::: info Изменено в 2.0
В 1.x было два уровня — middleware воркера и middleware маршрута. В 2.x остались только middleware маршрута, и они видят и сырое, и десериализованное сообщение. См. [Переход с 1.x](/ru/docs/2.x/upgrade#middleware).
:::

## Как написать middleware

```php
use KafkaBus\Core\Consumers\Messages\ConsumerMessageInterface;
use KafkaBus\Core\Pipelines\PipelineInterface;
use KafkaBus\Core\Receivers\Pipelines\ReceiverPipelineHandler;
use KafkaBus\Core\Receivers\Pipelines\ReceiverPipelineMiddleware;

final class LoggingMiddleware implements ReceiverPipelineMiddleware
{
    /**
     * @param PipelineInterface<ConsumerMessageInterface, ReceiverPipelineHandler> $pipeline
     * @return PipelineInterface<ConsumerMessageInterface, ReceiverPipelineHandler>
     */
    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        $message = $pipeline->handler()->target();
        $start = hrtime(true);

        $pipeline->continue();

        $elapsed = (hrtime(true) - $start) / 1e6;

        echo "Обработано за {$elapsed}мс: {$message->topicName()}" . PHP_EOL;

        return $pipeline;
    }
}
```

`PipelineInterface` параметризован двумя типами: результатом и классом обработчика.

### Методы обработчика pipeline

| Метод | Возвращает | Описание |
|---|---|---|
| `target()` | `ConsumerMessageInterface` | Сырое сообщение: payload, заголовки, топик, ключ, оффсет |
| `formatted()` | `mixed` | То, что получит обработчик, — результат фабрики сообщений маршрута |
| `handle()` | `ConsumerMessageInterface` | Вызывает обработчик (вызывается pipeline) |

Сообщение десериализуется до запуска pipeline, поэтому `formatted()` доступен в любом middleware. Тип зависит от фабрики маршрута: для обработчика с `#[MessageFactory(new DomainMessageFactory(OrderMessage::class))]` это `OrderMessage`, для аргумента типа `array` — декодированный JSON.

### Пример: фильтрация по бизнес-полям

```php
use KafkaBus\Messages\DomainEventEnum;

final class SkipDeletedMiddleware implements ReceiverPipelineMiddleware
{
    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        $order = $pipeline->handler()->formatted();

        if ($order instanceof OrderMessage && $order->getEvent() === DomainEventEnum::Delete) {
            return $pipeline; // обработчик не вызывается
        }

        return $pipeline->continue();
    }
}
```

::: tip
`formatted()` возвращает `mixed`. Если middleware написан под конкретный топик, тип известен; если переиспользуется между топиками — проверяйте через `instanceof`.
:::

## Middleware с обработкой ошибок

По умолчанию исключение из обработчика останавливает воркер. Чтобы этого не происходило, перехватите его в middleware:

```php
use Psr\Log\LoggerInterface;
use Throwable;

final class ErrorHandlingMiddleware implements ReceiverPipelineMiddleware
{
    public function __construct(private readonly LoggerInterface $logger)
    {
    }

    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        $message = $pipeline->handler()->target();

        try {
            return $pipeline->continue();
        } catch (Throwable $exception) {
            $this->logger->error('Message processing failed', [
                'topic' => $message->topicName(),
                'partition' => $message->original()->partition,
                'offset' => $message->original()->offset,
                'payload' => $message->payload(),
                'exception' => $exception,
            ]);

            return $pipeline;
        }
    }
}
```

::: warning
Проглатывая исключение, вы соглашаетесь, что сообщение закоммитится как обработанное и больше не вернётся: `dispatch()` завершается штатно, и воркер коммитит оффсет.

Если сообщение нужно обработать повторно, не глушите исключение — используйте [`ConsumerCommiterMiddleware`](/ru/docs/2.x/components/commiter) со счётчиком попыток или отправьте payload в DLQ-топик перед возвратом.
:::

::: tip
Внутри pipeline исключение обработчика приходит как есть. В `MessageConsumerNotHandledException` его оборачивает уже `Receiver::dispatch()`, после pipeline.
:::

Ошибки фабрики сообщений возникают до запуска pipeline, поэтому middleware их не перехватит — например, невалидный JSON для обработчика с аргументом `array`.

## Пропуск сообщения

Если middleware вернул pipeline без вызова `continue()`, обработчик не вызывается, но сообщение всё равно коммитится — с точки зрения воркера `dispatch()` завершился успешно. Так `ConsumerCommiterMiddleware` пропускает дубли.

```php
final class SkipTombstonesMiddleware implements ReceiverPipelineMiddleware
{
    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        if ($pipeline->handler()->target()->payload() === '') {
            return $pipeline; // tombstone — пропускаем, но оффсет сдвигаем
        }

        return $pipeline->continue();
    }
}
```

## Подключение к маршруту

```php
use KafkaBus\Core\Receivers\Routing\ReceiverBuilder;
use KafkaBus\Core\Receivers\Routing\RouteInfo;

$receiver = ReceiverBuilder::make($topicRegistry)
    ->add(new RouteInfo(
        topicKey: 'orders',
        handler: new OrderHandler(),
        middleware: [
            new ErrorHandlingMiddleware($logger),
            new LoggingMiddleware(),
            new SkipDeletedMiddleware(),
        ],
    ))
    ->build();
```

Чтобы применить одни и те же middleware ко всем топикам, добавьте их в каждый маршрут — например, через общий массив:

```php
$common = [new ErrorHandlingMiddleware($logger), new LoggingMiddleware()];

$receiver = ReceiverBuilder::make($topicRegistry)
    ->add(new RouteInfo('orders', new OrderHandler(), $common))
    ->add(new RouteInfo('products', new ProductHandler(), $common))
    ->build();
```

::: tip
В интеграции с Laravel глобальные middleware из `consumers.middleware` добавляются ко всем маршрутам автоматически. См. [Конфигурация Laravel](/ru/docs/2.x/laravel/configuration#консьюмеры).
:::

## Порядок выполнения

Middleware выполняются в порядке объявления. Код до `continue()` выполняется «на входе», код после — «на выходе»:

```
ErrorHandling → Logging → SkipDeleted → OrderHandler
```

Экземпляры middleware переиспользуются между сообщениями — не храните в них состояние конкретного сообщения.

## Встроенные middleware из экосистемы

| Middleware | Пакет | Описание |
|---|---|---|
| `ConsumerCommiterMiddleware` | `kafka-bus/commiter` | Идемпотентность, защита от дублей, лимит попыток |

Подробнее — в разделе [Commiter](/ru/docs/2.x/components/commiter).
