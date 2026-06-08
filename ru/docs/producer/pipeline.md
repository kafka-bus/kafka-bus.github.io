# Pipeline

## Producer Middleware

### Интерфейс

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

### Подключение к маршруту продьюсера

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

## Встроенные middleware из экосистемы

| Middleware                      | Пакет                | Описание                              |
|---------------------------------|----------------------|---------------------------------------|
| `ProducerIdempotencyMiddleware` | `kafka-bus-commiter` | Добавляет заголовок `x-idempotency-key` |
