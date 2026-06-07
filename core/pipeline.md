# Pipeline (Middleware)

Пакет реализует паттерн Middleware через конвейер (`Pipeline`). Middleware можно добавлять на трёх уровнях: глобально для воркера, для конкретного маршрута топика, и для маршрута producer'а.

## Как работает конвейер

Каждое входящее или исходящее сообщение проходит через цепочку middleware по порядку. Каждый middleware получает сообщение и callable `$next`, который передаёт управление следующему звену:

```
сообщение → Middleware1 → Middleware2 → Middleware3 → Handler
                                                          ↓
           Middleware1 ← Middleware2 ← Middleware3 ← ответ
```

## Consumer middleware

### Интерфейс

```php
use Micromus\KafkaBus\Interfaces\Consumers\Pipelines\ConsumerPipelineInterface;
use Micromus\KafkaBus\Interfaces\Consumers\Messages\ConsumerMessageInterface;

class LoggingMiddleware implements ConsumerPipelineInterface
{
    public function handle(ConsumerMessageInterface $message, callable $next): void
    {
        $start = hrtime(true);

        $next($message);

        $elapsed = (hrtime(true) - $start) / 1e6;
        echo "Обработано за {$elapsed}мс: {$message->topicName()}" . PHP_EOL;
    }
}
```

### Middleware с обработкой ошибок

```php
class RetryMiddleware implements ConsumerPipelineInterface
{
    public function handle(ConsumerMessageInterface $message, callable $next): void
    {
        try {
            $next($message);
        } catch (\Throwable $e) {
            // Логируем ошибку, но не прекращаем работу воркера
            logger()->error('Ошибка обработки сообщения', [
                'topic'   => $message->topicName(),
                'payload' => $message->payload(),
                'error'   => $e->getMessage(),
            ]);
        }
    }
}
```

### Применение на уровне воркера

Middleware применяется ко всем топикам воркера:

```php
use Micromus\KafkaBus\Bus\Listeners\Workers\Options;

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

### Применение на уровне маршрута топика

Middleware применяется только к конкретному топику:

```php
use Micromus\KafkaBus\Consumers\Router\RouteInfo;
use Micromus\KafkaBus\Consumers\Router\RouteOptions;

$consumerRoutes = ConsumerRoutesBuilder::make($topicRegistry)
    ->add(new RouteInfo(
        topicKey: 'orders',
        handler:  new OrderHandler(),
        options:  new RouteOptions(
            middleware: [new TenantContextMiddleware()]
        )
    ))
    ->build();
```

## Producer middleware

### Интерфейс

```php
use Micromus\KafkaBus\Interfaces\Producers\Pipelines\ProducerPipelineInterface;
use Micromus\KafkaBus\Interfaces\Producers\Messages\ProducerMessageInterface;

class HeadersMiddleware implements ProducerPipelineInterface
{
    public function handle(ProducerMessageInterface $message, callable $next): void
    {
        // Добавляем заголовки перед отправкой
        $message = $message->withHeaders(array_merge(
            $message->getHeaders(),
            ['x-sent-at' => now()->toIso8601String()]
        ));

        $next($message);
    }
}
```

### Применение на маршрут publisher'а

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

## Порядок выполнения

Middleware выполняются в порядке добавления. Для consumer:

```
[воркер middleware] → [маршрут middleware] → [handler]
LoggingMiddleware  → TenantMiddleware     → OrderHandler
```

## Готовые middleware из экосистемы

| Middleware | Пакет | Описание |
|---|---|---|
| `ConsumerCommiterMiddleware` | `kafka-bus-commiter` | Идемпотентность, защита от дублей |
| `ProducerIdempotencyMiddleware` | `kafka-bus-commiter` | Добавляет `x-idempotency-key` заголовок |

::: tip
Подробнее о `ConsumerCommiterMiddleware` — в разделе [Commiter](/packages/commiter).
:::
