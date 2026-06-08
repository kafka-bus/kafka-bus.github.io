# Pipeline

При обработке сообщения запускаются 2 pipeline:
 - на уровне Consumer (получает `ConsumerMessageInterface`, конкретно `WorkerConsumerMessage`)
 - на уровне Route (получает объект, основанный на типе аргумента обработчика)

## Consumer Middleware

### Интерфейс

```php
use KafkaBus\Core\Consumers\Pipelines\ConsumerPipelineMiddleware;
use KafkaBus\Core\Consumers\Pipelines\ConsumerPipelineHandler;

class LoggingMiddleware implements ConsumerPipelineMiddleware
{
    /**
     * @param PipelineInterface<ConsumerPipelineHandler> $pipeline
     * @return PipelineInterface<ConsumerPipelineHandler>
     */
    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        $start = hrtime(true);

        $pipeline->continue();
        $message = $pipeline->handler()->target();

        $elapsed = (hrtime(true) - $start) / 1e6;
        
        echo "Обработано за {$elapsed}мс: {$message->topicName()}" . PHP_EOL;
        
        return $pipeline;
    }
}
```

### Middleware с обработкой ошибок

```php
class LoggerMiddleware implements ConsumerPipelineMiddleware
{
    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        try {
            return $pipeline->continue();
        } catch (\Throwable $e) {
            // Логируем ошибку без остановки воркера
            logger()->error('Ошибка обработки сообщения', [
                'topic'   => $message->topicName(),
                'payload' => $message->payload(),
                'error'   => $e->getMessage(),
            ]);
            
            return $pipeline;
        }
    }
}
```

::: tip
Поглощая исключение, воркер продолжает обрабатывать следующие сообщения, и они будут закоммичены в Apache Kafka.
:::

### Подключение

Middleware применяется ко всем топикам воркера:

```php
use KafkaBus\Core\Bus\Listeners\Workers\Options;

$workerRegistry = MemoryWorkerRegistry::make()
    ->add(new Worker(
        name:    'default',
        routes:  $consumerRoutes,
        options: new Options(
            middleware: [
                new LoggingMiddleware(),
                new RetryMiddleware(),
            ]
        )
    ));
```

## Route Middleware

### Интерфейс

Предположим, есть обработчик заказов `OrderHandler`:

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

```php
use KafkaBus\Core\Consumers\Router\MessagePipelineMiddleware;
use KafkaBus\Core\Consumers\Pipelines\MessagePipelineHandler;

class CheckingMiddleware implements MessagePipelineMiddleware
{
    /**
     * @param PipelineInterface<MessagePipelineHandler> $pipeline
     * @return PipelineInterface<MessagePipelineHandler>
     */
    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        $message = $pipeline->handler()->target(); // OrderMessage
        
        // Что-то делаем
        
        return $pipeline->continue();
    }
}
```

### Подключение

Middleware применяется только к конкретному топику:

```php
use Micromus\KafkaBus\Consumers\Router\RouteInfo;
use Micromus\KafkaBus\Consumers\Router\RouteOptions;

$consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry)
    ->add(new RouteInfo(
        topicKey: 'orders',
        handler:  new OrderHandler(),
        options:  new RouteOptions(
            middleware: [new CheckingMiddleware()]
        )
    ))
    ->build();
```

## Порядок выполнения

Middleware выполняются в порядке добавления. Для консьюмера:

```
[worker middleware] → [route middleware] → [handler]
LoggingMiddleware   → CheckingMiddleware → OrderHandler
```

## Встроенные middleware из экосистемы

| Middleware                      | Пакет                | Описание                            |
|---------------------------------|----------------------|-------------------------------------|
| `ConsumerCommiterMiddleware`    | `kafka-bus/commiter` | Идемпотентность, защита от дублей   |

::: tip
Подробнее о `ConsumerCommiterMiddleware` — в разделе [Commiter](/ru/docs/components/commiter).
:::
