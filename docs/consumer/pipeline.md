# Pipeline

Processing a message runs two independent pipelines:

| Pipeline     | Configured in                          | Middleware interface          | What's in `target()`             |
|--------------|-------------------------------------------|----------------------------------|--------------------------------------|
| **Consumer** | `Workers\Options::$middleware`          | `ConsumerPipelineMiddleware`  | `ConsumerMessageInterface`         |
| **Route**    | `RouteInfo::$middleware`                | `MessagePipelineMiddleware`   | The `MessageFactory` result         |

The Consumer pipeline is shared across all of the worker's topics and sees the raw Kafka message. The Route pipeline is configured per topic and sees the already-deserialized object.

```
ConsumerMessage
    │
    ▼ [worker's Consumer middleware]
ConsumerRouter → RouteExecutor
    │ MessageFactory: ConsumerMessage → OrderMessage
    ▼ [route's Route middleware]
Handler
```

## Consumer Middleware

### Interface

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

        echo "Processed in {$elapsed}ms: {$message->topicName()}" . PHP_EOL;

        return $pipeline;
    }
}
```

`PipelineInterface` is parameterized by two types: the result and the handler class.

### Middleware with Error Handling

By default, an exception from the handler stops the worker. To prevent that, catch it in middleware:

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
            $this->logger->error('Message processing failed', [
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
By swallowing the exception, you're accepting that the message will be **committed as processed** and won't come back. The `ConsumerStream` loop calls `commit()` right after `handle()`, as long as it didn't throw.

If the message needs to be retried, don't swallow the exception here — use [`ConsumerCommiterMiddleware`](/docs/components/commiter) with an attempt counter, or send the payload to a DLQ topic before returning the pipeline.
:::

::: tip
Exceptions from the handler reach middleware already wrapped in a `MessageConsumerNotHandledException`. The original is available via `$e->getPrevious()`.
:::

### Stopping Processing

If a middleware returns the pipeline **without** `continue()`, the handler never runs, but the message is still committed — from `ConsumerStream`'s point of view, the iteration completed successfully. This is how duplicate skipping works in `ConsumerCommiterMiddleware`.

```php
final class SkipTombstonesMiddleware implements ConsumerPipelineMiddleware
{
    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        if ($pipeline->handler()->target()->payload() === '') {
            return $pipeline; // tombstone — skip it, but still advance the offset
        }

        return $pipeline->continue();
    }
}
```

### Attaching

Middleware applies to all of the worker's topics:

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

## Route Middleware

Route middleware sees the object the handler will receive. Say there's an order handler:

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

### Interface

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
            return $pipeline; // don't pass through to the handler
        }

        return $pipeline->continue();
    }
}
```

::: tip
`target()` returns `mixed` — the actual type depends on the route's `MessageFactory`. If a middleware is written for a specific topic, the type is known; if it's reused across topics, check it with `instanceof`.
:::

### Attaching

Middleware applies only to a specific topic:

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

## Execution Order

Middleware run in the order they're added, the entire Consumer pipeline first, then the Route pipeline:

```
[worker middleware]  →  [route middleware]  →  [handler]
ErrorHandling         →  Checking               →  OrderHandler
LoggingMiddleware
```

Just like on the producer side, code before `continue()` runs on the "way in", code after it runs on the "way back". Middleware instances are reused across messages — don't store per-message state in them.

## Choosing a Level

| Task                                             | Level      |
|-----------------------------------------------------|-------------|
| Logging, metrics, tracing                           | Consumer   |
| Error handling, DLQ                                 | Consumer   |
| Idempotence (`ConsumerCommiterMiddleware`)          | Consumer   |
| Access to the raw payload and headers                | Consumer   |
| Validating the deserialized object                   | Route      |
| Filtering by business fields                          | Route      |
| Multi-tenancy based on message data                   | Route      |

## Built-in Ecosystem Middleware

| Middleware                    | Package               | Description                            |
|----------------------------------|--------------------------|---------------------------------------------|
| `ConsumerCommiterMiddleware`  | `kafka-bus/commiter`   | Idempotence, duplicate protection            |

::: tip
More details on `ConsumerCommiterMiddleware` — in [Commiter](/docs/components/commiter).
:::
