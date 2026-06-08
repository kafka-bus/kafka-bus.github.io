# Pipeline

During message processing, 2 pipelines are invoked:
 - at the Consumer level (receives `ConsumerMessageInterface`, specifically `WorkerConsumerMessage`)
 - at the Route level (receives an object based on the Handler argument type)

## Consumer Middleware

### Interface

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
        
        echo "Processed in {$elapsed}ms: {$message->topicName()}" . PHP_EOL;
        
        return $pipeline;
    }
}
```

### Middleware with Error Handling

```php
class LoggerMiddleware implements ConsumerPipelineMiddleware
{
        public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        try {
            return $pipeline->continue();
        } catch (\Throwable $e) {
            // Log the error without stopping the worker
            logger()->error('Message processing error', [
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
By swallowing the exception, the worker continues processing the next message and messages will be committed to Apache Kafka.
:::

### Applying

Middleware is applied to all topics in the worker:

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

### Interface

Suppose there is an order handler `OrderHandler`:

```php
class OrderHandler
{
    #[MessageFactory(new DomainMessageFactory(OrderMessage::class))]
    public function __invoke(OrderMessage $order): void
    {
        // Order processing logic
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
        
        // Something
        
        return $pipeline->continue();
    }
}
```

### Applying

Middleware is applied only to a specific topic:

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

## Execution Order

Middleware executes in the order it was added. For a consumer:

```
[worker middleware] → [route middleware] → [handler]
LoggingMiddleware   → CheckingMiddleware → OrderHandler
```

## Built-in Middleware from the Ecosystem

| Middleware                      | Package              | Description                             |
|---------------------------------|----------------------|-----------------------------------------|
| `ConsumerCommiterMiddleware`    | `kafka-bus/commiter` | Idempotency, duplicate protection       |

::: tip
Learn more about `ConsumerCommiterMiddleware` in the [Commiter](/docs/components/commiter) section.
:::
