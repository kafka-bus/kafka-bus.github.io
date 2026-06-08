# Commiter

Middleware-пакет для [kafka-bus](https://github.com/kafka-bus/kafka-bus), обеспечивающий идемпотентную обработку сообщений: отслеживает обработанные сообщения, предотвращает дубликаты и ограничивает количество попыток обработки.

## Установка

```bash
composer require kafka-bus/commiter
```

## Как это работает

`ConsumerCommiterMiddleware` встраивается в pipeline консьюмера и обрабатывает четыре сценария:

| Сценарий                                            | Действие                                          |
|-----------------------------------------------------|---------------------------------------------------|
| Сообщение уже обработано (`commitedAt != null`)     | Логирует предупреждение, останавливает pipeline   |
| Превышен `maxAttempt`                               | Логирует ошибку, останавливает pipeline           |
| Сообщение обработано успешно                        | Вызывает `commit()`, помечает как обработанное    |
| Обработчик выбросил исключение                         | Вызывает `failed()`, перевыбрасывает исключение   |

## Базовое использование

### 1. Реализуйте репозиторий

Реализуйте `RepositorySourceInterface` для хранения состояния сообщений (БД, Redis и т.д.):

```php
use KafkaBus\Commiter\Attempt;
use KafkaBus\Commiter\Interfaces\RepositorySourceInterface;

class DatabaseRepositorySource implements RepositorySourceInterface
{
    /**
     * Возвращает текущее состояние ключа.
     * Null означает, что сообщение ещё не встречалось.
     */
    public function get(string $key): ?Attempt
    {
        $record = DB::table('kafka_commits')->where('key', $key)->first();

        if (!$record) {
            return null;
        }

        return new Attempt(
            attempts: $record->attempts + 1,
            committedAt: $record->committed_at ? new DateTime($record->committed_at) : null,
        );
    }

    /**
     * Увеличивает счётчик неудачных попыток.
     */
    public function increment(string $key): void
    {
        DB::table('kafka_commits')->upsert(
            ['key' => $key, 'attempts' => 1],
            ['key'],
            ['attempts' => DB::raw('attempts + 1')]
        );
    }

    /**
     * Помечает ключ как успешно обработанный.
     */
    public function commit(string $key): void
    {
        DB::table('kafka_commits')->upsert(
            ['key' => $key, 'committed_at' => now(), 'attempts' => 1],
            ['key'],
            ['committed_at' => now()]
        );
    }
}
```

### 2. Подключите middleware

```php
use KafkaBus\Commiter\Middleware\ConsumerCommiterMiddleware;
use KafkaBus\Commiter\Repositories\NativeMessageRepository;

$repository = new NativeMessageRepository(new DatabaseRepositorySource());

$workerRegistry = MemoryWorkerRegistry::make()
    ->add(new Worker(
        name:    'default',
        routes:  $consumerRoutes,
        options: new Options(
            middleware: [
                new ConsumerCommiterMiddleware(
                    repository: $repository,
                    logger:     new NullLogger(),  // PSR-3
                    maxAttempt: 3,           // -1 = безлимитно
                )
            ]
        )
    ));
```

### Параметры middleware

| Параметр     | Тип                         | По умолчанию | Описание                              |
|--------------|-----------------------------|--------------|---------------------------------------|
| `repository` | `RepositorySourceInterface` | —            | Хранилище состояний                   |
| `logger`     | `LoggerInterface`           | `NullLogger` | PSR-3 совместимый логгер              |
| `maxAttempt` | `int`                       | `-1`         | Макс. попыток. `-1` — безлимитно      |

## Ключи идемпотентности

По умолчанию ключ дедупликации — это Kafka `msgId()`, комбинация топика, партиции и offset. Это работает, пока одно физическое сообщение не появляется с другим offset.

**Проблема:** при повторных отправках, ре-отправке или межкластерном зеркалировании одно логическое событие может прийти с другим `msgId()` и обойти проверку.

**Решение:** ключ идемпотентности — стабильный идентификатор, который продьюсер добавляет в заголовок `x-idempotency-key`. Консьюмер использует его вместо `msgId()`.

### Выбор правильного ключа

✅ Хорошие варианты:
- `order-42-v3` (aggregate id + версия)
- id строки в таблице outbox
- любое значение, которое апстрим считает уникальным для события

❌ Плохие варианты:
- timestamp (меняется при повторной отправке)
- случайный UUID, генерируемый при каждой отправке

## ProducerIdempotencyMiddleware

### Реализуйте HasIdempotency на сообщении

```php
use KafkaBus\Core\Interfaces\Producers\Messages\ProducerMessageInterface;
use KafkaBus\Commiter\Interfaces\HasIdempotency;

final readonly class ProductCreatedMessage implements ProducerMessageInterface, HasIdempotency
{
    public function __construct(
        private string $productId,
        private string $payload,
    ) {}

    public function toPayload(): string
    {
        return $this->payload;
    }

    public function getIdempotencyKey(): string
    {
        return $this->productId; // стабильный ключ
    }
}
```

### Подключите middleware к маршруту продьюсера

```php
use KafkaBus\Core\Bus\Publishers\Router\Options;
use KafkaBus\Commiter\Middleware\ProducerIdempotencyMiddleware;

$publisherRoutes = PublisherRoutesBuilder::make($topicRegistry)
    ->add(
        messageClass: ProductCreatedMessage::class,
        topicKey:     'products',
        options:      new Options(
            middleware: [new ProducerIdempotencyMiddleware()]
        )
    )
    ->build();
```

Middleware добавляет заголовок `x-idempotency-key` к каждому исходящему сообщению. Если сообщение не реализует `HasIdempotency`, заголовок не добавляется.

## IdempotencyMessageRepository

На стороне консьюмера `IdempotencyMessageRepository` читает заголовок `x-idempotency-key` и строит ключ хранения как `"{header}-{topicName}"`. Одинаковый ключ идемпотентности в разных топиках представляет разные события.

При отсутствии заголовка используется `msgId()`, поэтому старые продьюсеры без идемпотентности работают без изменений.

```php
use KafkaBus\Commiter\Middleware\ConsumerCommiterMiddleware;
use KafkaBus\Commiter\Repositories\IdempotencyMessageRepository;

$repository = new IdempotencyMessageRepository(new DatabaseRepositorySource());

new ConsumerCommiterMiddleware($repository, maxAttempt: 3);
```
