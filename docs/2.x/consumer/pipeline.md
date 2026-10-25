---
title: Consumer pipeline
description: Middleware for incoming messages — access to raw and deserialized messages, error handling, skipping, and execution order
---

# Consumer pipeline

Every incoming message passes through its route's pipeline: a chain of middleware, at the end of which the handler is called. Middleware is attached per route and implements `ReceiverPipelineMiddleware`.

```
ConsumerMessage
    │
    ▼ MessageFactory: ConsumerMessage → handler argument
    ▼ Middleware1 → Middleware2 → ReceiverPipelineHandler::handle()
                                        │
                                        ▼
                                     Handler
```

::: info Changed in 2.0
In 1.x there were two levels — worker middleware and route middleware. In 2.x there is only route middleware, and it sees both the raw and the deserialized message. See [Upgrading from 1.x](/docs/2.x/upgrade#middleware).
:::

## Writing a middleware

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

        echo "Processed in {$elapsed}ms: {$message->topicName()}" . PHP_EOL;

        return $pipeline;
    }
}
```

`PipelineInterface` is parameterized by two types: the result and the handler class.

### Pipeline handler methods

| Method | Returns | Description |
|---|---|---|
| `target()` | `ConsumerMessageInterface` | The raw message: payload, headers, topic, key, offset |
| `formatted()` | `mixed` | What the handler will receive — the result of the route's message factory |
| `handle()` | `ConsumerMessageInterface` | Calls the handler (called by the pipeline) |

The message is deserialized before the pipeline starts, so `formatted()` is available in every middleware. Its type depends on the route's factory: for a handler with `#[MessageFactory(new DomainMessageFactory(OrderMessage::class))]` it's an `OrderMessage`, for an `array` argument it's the decoded JSON.

### Example: filtering by business fields

```php
use KafkaBus\Messages\DomainEventEnum;

final class SkipDeletedMiddleware implements ReceiverPipelineMiddleware
{
    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        $order = $pipeline->handler()->formatted();

        if ($order instanceof OrderMessage && $order->getEvent() === DomainEventEnum::Delete) {
            return $pipeline; // the handler isn't called
        }

        return $pipeline->continue();
    }
}
```

::: tip
`formatted()` returns `mixed`. If a middleware is written for a specific topic, the type is known; if it's reused across topics, check it with `instanceof`.
:::

## Middleware with error handling

By default, an exception from the handler stops the worker. To prevent that, catch it in a middleware:

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
By swallowing the exception, you accept that the message is committed as processed and won't come back: `dispatch()` returns normally, and the worker commits the offset.

If the message needs to be retried, don't swallow the exception — use [`ConsumerCommiterMiddleware`](/docs/2.x/components/commiter) with an attempt counter, or send the payload to a DLQ topic before returning.
:::

::: tip
Inside the pipeline, the handler's exception arrives as is. It's wrapped in a `MessageConsumerNotHandledException` only after the pipeline, in `Receiver::dispatch()`.
:::

Errors of the message factory happen before the pipeline starts, so a middleware can't catch them — for example, invalid JSON for an `array` handler.

## Skipping a message

If a middleware returns the pipeline without calling `continue()`, the handler isn't called, but the message is still committed — from the worker's point of view, `dispatch()` succeeded. This is how `ConsumerCommiterMiddleware` skips duplicates.

```php
final class SkipTombstonesMiddleware implements ReceiverPipelineMiddleware
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

## Attaching to a route

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

To apply the same middleware to every topic, add it to every route — for example, with a shared array:

```php
$common = [new ErrorHandlingMiddleware($logger), new LoggingMiddleware()];

$receiver = ReceiverBuilder::make($topicRegistry)
    ->add(new RouteInfo('orders', new OrderHandler(), $common))
    ->add(new RouteInfo('products', new ProductHandler(), $common))
    ->build();
```

::: tip
In the Laravel integration, global middleware from `consumers.middleware` is added to every route automatically. See [Laravel configuration](/docs/2.x/laravel/configuration#consumers).
:::

## Execution order

Middleware run in the order they're declared. Code before `continue()` runs on the way in, code after it on the way back:

```
ErrorHandling → Logging → SkipDeleted → OrderHandler
```

Middleware instances are reused across messages — don't store per-message state in them.

## Built-in ecosystem middleware

| Middleware | Package | Description |
|---|---|---|
| `ConsumerCommiterMiddleware` | `kafka-bus/commiter` | Idempotence, duplicate protection, attempt limit |

More details — in [Commiter](/docs/2.x/components/commiter).
