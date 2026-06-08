# Commiter

Middleware-пакет для [kafka-bus](https://github.com/kafka-bus/kafka-bus), обеспечивающий идемпотентную обработку сообщений: отслеживает обработанные сообщения, предотвращает дублирование и ограничивает количество попыток обработки.

## Установка

```bash
composer require kafka-bus/commiter
```

## Как работает

`ConsumerCommiterMiddleware` встраивается в consumer-конвейер и обрабатывает четыре сценария:

| Сценарий                                        | Действие                                        |
|-------------------------------------------------|-------------------------------------------------|
| Сообщение уже обработано (`commitedAt != null`) | Логирует предупреждение, останавливает конвейер |
| Превышено `maxAttempt`                          | Логирует ошибку, останавливает конвейер         |
| Сообщение обработано успешно                    | Вызывает `commit()`, фиксирует как обработанное |
| Обработчик выбросил исключение                  | Вызывает `failed()`, перебрасывает исключение   |

## Базовое использование

### 1. Реализуйте хранилище

Необходимо реализовать `RepositorySourceInterface` для хранения состояния сообщений (БД, Redis и т.д.):

```php
use KafkaBus\Commiter\Attempt;
use KafkaBus\Commiter\Interfaces\RepositorySourceInterface;

class DatabaseRepositorySource implements RepositorySourceInterface
{
    /**
     * Возвращает текущее состояние ключа.
     * Null — сообщение ещё не встречалось.
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
                    maxAttempt: 3,           // -1 = без ограничений
                )
            ]
        )
    ));
```

### Параметры middleware

| Параметр     | Тип                         | По умолчанию | Описание                                 |
|--------------|-----------------------------|--------------|------------------------------------------|
| `repository` | `RepositorySourceInterface` | —            | Хранилище состояния                      |
| `logger`     | `LoggerInterface`           | `NullLogger` | PSR-3 совместимый логгер                 |
| `maxAttempt` | `int`                       | `-1`         | Максимум попыток. `-1` — без ограничений |

## Idempotency Keys

По умолчанию ключом дедупликации служит `msgId()` Kafka — комбинация топика, партиции и офсета. Это работает при условии, что одно физическое сообщение не появится с другим офсетом.

**Проблема:** при ретраях, повторных отправках или cross-cluster mirroring то же логическое событие может прийти с другим `msgId()` и пройти мимо проверки.

**Решение:** idempotency key — стабильный идентификатор, который producer добавляет в заголовок `x-idempotency-key`. Consumer использует его вместо `msgId()`.

### Выбор хорошего ключа

✅ Хорошие варианты:
- `order-42-v3` (aggregate id + версия)
- id строки из outbox-таблицы
- любое значение, которое upstream считает уникальным для события

❌ Плохие варианты:
- timestamp (меняется при ретрае)
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

### Подключите middleware в маршрут publisher'а

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

Middleware добавит заголовок `x-idempotency-key` к каждому исходящему сообщению. Если сообщение не реализует `HasIdempotency`, заголовок не добавляется.

## IdempotencyMessageRepository

На стороне consumer'а `IdempotencyMessageRepository` читает заголовок `x-idempotency-key` и строит ключ хранилища как `"{header}-{topicName}"`. Один и тот же idempotency key в разных топиках — это разные события.

Если заголовок отсутствует — fallback на `msgId()`, поэтому старые producer'ы без idempotency работают без изменений.

```php
use KafkaBus\Commiter\Middleware\ConsumerCommiterMiddleware;
use KafkaBus\Commiter\Repositories\IdempotencyMessageRepository;

$repository = new IdempotencyMessageRepository(new DatabaseRepositorySource());

new ConsumerCommiterMiddleware($repository, maxAttempt: 3);
```

