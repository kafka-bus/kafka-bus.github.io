# Pipeline

В процессе обработки сообщений будет вызвано 2 pipeline:
 - на уровне Consumer (передаётся ConsumerMessageInterface, в частности WorkerConsumerMessage)
 - на уровне Route (передается объект исходя из аргумента Handler)

## Consumer middleware

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
            // Логируем ошибку, но не прекращаем работу воркера
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
Игнорируя Exception, воркер продолжит обработку следующего сообщения и сообщения будут закомичены в Apache Kafka.
:::

### Применение

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

## Route middleware

### Интерфейс

Допустим есть обработчик заказов OrderHandler:

```php
class OrderHandler
{
    #[MessageFactory(new DomainMessageFactory(OrderMessage::class))]
    public function __invoke(OrderMessage $order): void
    {
        // Логика обработки заказов
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
        
        // Что то
        
        return $pipeline->continue();
    }
}
```

### Применение

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

Middleware выполняются в порядке добавления. Для consumer:

```
[воркер middleware] → [маршрут middleware] → [handler]
LoggingMiddleware   → CheckingMiddleware   → OrderHandler
```

## Готовые middleware из экосистемы

| Middleware                      | Пакет                | Описание                                |
|---------------------------------|----------------------|-----------------------------------------|
| `ConsumerCommiterMiddleware`    | `kafka-bus/commiter` | Идемпотентность, защита от дублей       |

::: tip
Подробнее о `ConsumerCommiterMiddleware` — в разделе [Commiter](/docs/components/commiter).
:::