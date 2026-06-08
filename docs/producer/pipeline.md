# Pipeline

## Producer Middleware

### Interface

```php
use KafkaBus\Core\Producers\Pipelines\ProducerPipelineMiddleware;
use KafkaBus\Core\Producers\Pipelines\ProducerPipelineHandler;

class HeadersMiddleware implements ProducerPipelineMiddleware
{
    /**
     * @param PipelineInterface<ProducerPipelineHandler> $pipeline
     * @return PipelineInterface<ProducerPipelineHandler>
     */
    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        $pipeline->handler()
            ->withHeaders(['x-sent-at' => now()->toIso8601String()]);
            
        return $pipeline->continue();
    }
}
```

### Applying to a Publisher Route

```php
use Micromus\KafkaBus\Bus\Publishers\Router\Options;

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

## Built-in Middleware from the Ecosystem

| Middleware                      | Package              | Description                                   |
|---------------------------------|----------------------|-----------------------------------------------|
| `ProducerIdempotencyMiddleware` | `kafka-bus-commiter` | Adds `x-idempotency-key` header               |
