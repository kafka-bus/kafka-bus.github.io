---
title: Producer pipeline
description: Middleware for outgoing messages — adding headers, canceling a send, execution order
---

# Pipeline

Every outgoing message passes through its route's pipeline: a chain of middleware, at the end of which `PublisherPipelineHandler` assembles the final `ProducerMessage`.

```
ProducerMessageInterface
    │
    ▼ Middleware1 → Middleware2 → PublisherPipelineHandler::handle()
                                        │
                                        ▼
                                 ProducerMessage → Producer
```

## Producer middleware

### Interface

```php
use KafkaBus\Core\Pipelines\PipelineInterface;
use KafkaBus\Core\Publishers\Pipelines\PublisherPipelineHandler;
use KafkaBus\Core\Publishers\Pipelines\PublisherPipelineMiddleware;
use KafkaBus\Core\Producers\Messages\ProducerMessage;

final class HeadersMiddleware implements PublisherPipelineMiddleware
{
    /**
     * @param PipelineInterface<ProducerMessage, PublisherPipelineHandler> $pipeline
     * @return PipelineInterface<ProducerMessage, PublisherPipelineHandler>
     */
    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        $pipeline->handler()
            ->withHeader('x-sent-at', (new DateTimeImmutable())->format(DATE_ATOM));

        return $pipeline->continue();
    }
}
```

::: warning
The method is called `withHeader(string $key, mixed $value)` and adds **one** header per call. There's no `withHeaders()` taking an array — for multiple headers, call `withHeader()` several times (it returns `$this`, so calls can be chained).
:::

`PipelineInterface` is parameterized by two types: the `ProducerMessage` result and the `PublisherPipelineHandler` handler class.

### Pipeline handler methods

| Method                                | Returns                     | Description                                           |
|-----------------------------------------|--------------------------------|------------------------------------------------------------|
| `target()`                           | `ProducerMessageInterface`  | The application's original message — before serialization  |
| `topic()`                            | `Topic`                     | The topic the message will be sent to                        |
| `withHeader(string $key, mixed $val)`| `self`                      | Adds a header to the final `ProducerMessage`                 |
| `handle()`                           | `ProducerMessage`           | Assembles the final message (called by the pipeline)          |

Headers added via `withHeader()` take priority over those returned by the message's own `HasHeaders::getHeaders()`.

### Example: enriching by message type

```php
final class TracingMiddleware implements PublisherPipelineMiddleware
{
    public function __construct(private readonly TraceContext $trace) {}

    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        $handler = $pipeline->handler();

        $handler
            ->withHeader('x-trace-id', $this->trace->traceId())
            ->withHeader('x-source', 'catalog-service')
            ->withHeader('x-topic', $handler->topic()->key);

        return $pipeline->continue();
    }
}
```

### Canceling a send

If a middleware returns the pipeline **without** calling `continue()`, the handler never runs and `start()` returns `null`. `PublisherStream` skips `null` results, so the message simply won't be sent:

```php
final class FeatureFlagMiddleware implements PublisherPipelineMiddleware
{
    public function __construct(private readonly FeatureFlags $flags) {}

    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        if (! $this->flags->enabled('kafka.publishing')) {
            return $pipeline; // the message won't reach Kafka
        }

        return $pipeline->continue();
    }
}
```

::: warning
The cancellation happens silently — no exception, no log entry. If tracking skipped messages matters for observability, log it yourself inside the middleware.
:::

### Execution order

Middleware run in the order they're declared in the array. Code before `continue()` runs on the "way in", code after it runs on the "way back":

```php
new Options(middleware: [
    new TracingMiddleware(),   // 1st in, 3rd out
    new HeadersMiddleware(),   // 2nd in, 2nd out
    new MetricsMiddleware(),   // 3rd in, 1st out
]);
```

Keep in mind: when publishing a batch, the pipeline runs **separately for each message**, but the middleware instance is reused. Don't store per-message state in it.

### Attaching to a producer route

```php
use KafkaBus\Core\Publishers\Routing\Options;
use KafkaBus\Core\Publishers\Routing\PublisherRoutesBuilder;

$publisherRoutes = PublisherRoutesBuilder::make($topicRegistry)
    ->add(
        messageClass: ProductCreatedMessage::class,
        topicKey:     'products',
        options:      new Options(
            middleware: [new HeadersMiddleware()]
        )
    )
    ->build();
```

::: tip
There's no global middleware for all routes at the core level — it's set per route. In the Laravel integration, the shared list from `producers.middleware` is automatically mixed into every route; see [Laravel Configuration](/docs/2.x/laravel/configuration#producers).
:::

## Built-in ecosystem middleware

| Middleware                      | Package               | Description                                                     |
|-----------------------------------|-------------------------|----------------------------------------------------------------------|
| `PublisherIdempotencyMiddleware` | `kafka-bus/commiter`  | Adds an `x-idempotency-key` header from `HasIdempotency`             |

More details — in [Commiter](/docs/2.x/components/commiter#idempotency-key-on-the-producer-side).
