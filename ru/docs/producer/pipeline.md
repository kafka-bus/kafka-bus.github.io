# Pipeline

Каждое исходящее сообщение проходит через pipeline своего маршрута: цепочку middleware, в конце которой `ProducerPipelineHandler` собирает финальный `ProducerMessage`.

```
ProducerMessageInterface
    │
    ▼ Middleware1 → Middleware2 → ProducerPipelineHandler::handle()
                                        │
                                        ▼
                                 ProducerMessage → Producer
```

## Producer Middleware

### Интерфейс

```php
use KafkaBus\Core\Interfaces\Pipelines\PipelineInterface;
use KafkaBus\Core\Producers\Pipelines\ProducerPipelineHandler;
use KafkaBus\Core\Producers\Pipelines\ProducerPipelineMiddleware;
use KafkaBus\Core\Producers\Messages\ProducerMessage;

final class HeadersMiddleware implements ProducerPipelineMiddleware
{
    /**
     * @param PipelineInterface<ProducerMessage, ProducerPipelineHandler> $pipeline
     * @return PipelineInterface<ProducerMessage, ProducerPipelineHandler>
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
Метод называется `withHeader(string $key, mixed $value)` и добавляет **один** заголовок за вызов. Метода `withHeaders()` с массивом не существует — для нескольких заголовков вызывайте `withHeader()` несколько раз (он возвращает `$this`, поэтому вызовы можно объединять в цепочку).
:::

`PipelineInterface` параметризован двумя типами: результатом `ProducerMessage` и классом обработчика `ProducerPipelineHandler`.

### API ProducerPipelineHandler

| Метод                                | Возвращает                  | Описание                                            |
|--------------------------------------|-----------------------------|-----------------------------------------------------|
| `target()`                           | `ProducerMessageInterface`  | Исходное сообщение приложения — до сериализации     |
| `topic()`                            | `Topic`                     | Топик, в который уйдёт сообщение                    |
| `withHeader(string $key, mixed $val)`| `self`                      | Добавляет заголовок к итоговому `ProducerMessage`   |
| `handle()`                           | `ProducerMessage`           | Собирает финальное сообщение (вызывается pipeline)  |

Заголовки, добавленные через `withHeader()`, имеют приоритет над теми, что вернул `HasHeaders::getHeaders()` самого сообщения.

### Пример: обогащение по типу сообщения

```php
final class TracingMiddleware implements ProducerPipelineMiddleware
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

### Отмена отправки

Если middleware вернёт pipeline **без** вызова `continue()`, обработчик не выполнится и `start()` вернёт `null`. `ProducerStream` пропускает `null`-результаты, поэтому сообщение просто не будет отправлено:

```php
final class FeatureFlagMiddleware implements ProducerPipelineMiddleware
{
    public function __construct(private readonly FeatureFlags $flags) {}

    public function handle(PipelineInterface $pipeline): PipelineInterface
    {
        if (! $this->flags->enabled('kafka.publishing')) {
            return $pipeline; // сообщение не уйдёт в Kafka
        }

        return $pipeline->continue();
    }
}
```

::: warning
Отмена происходит молча — ни исключения, ни записи в лог. Если пропуск сообщений важен для наблюдаемости, логируйте его сами внутри middleware.
:::

### Порядок выполнения

Middleware выполняются в порядке объявления в массиве. Код до `continue()` выполняется на «пути туда», после — на «пути обратно»:

```php
new Options(middleware: [
    new TracingMiddleware(),   // 1-й на входе, 3-й на выходе
    new HeadersMiddleware(),   // 2-й на входе, 2-й на выходе
    new MetricsMiddleware(),   // 3-й на входе, 1-й на выходе
]);
```

Учтите: при публикации батча pipeline запускается **для каждого сообщения отдельно**, но экземпляр middleware переиспользуется. Не храните в нём состояние конкретного сообщения.

### Подключение к маршруту продьюсера

```php
use KafkaBus\Core\Bus\Publishers\Router\Options;
use KafkaBus\Core\Bus\Publishers\Router\PublisherRoutesBuilder;

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
Глобального middleware для всех маршрутов на уровне ядра нет — он задаётся для каждого маршрута отдельно. В интеграции с Laravel общий список из `producers.middleware` автоматически подмешивается в каждый маршрут; см. [Конфигурация Laravel](/ru/docs/laravel/configuration#продьюсеры).
:::

## Встроенные middleware из экосистемы

| Middleware                      | Пакет                | Описание                                                       |
|---------------------------------|----------------------|----------------------------------------------------------------|
| `ProducerIdempotencyMiddleware` | `kafka-bus/commiter` | Добавляет заголовок `x-idempotency-key` из `HasIdempotency`     |

Подробнее — в разделе [Commiter](/ru/docs/components/commiter#produceridempotencymiddleware).
