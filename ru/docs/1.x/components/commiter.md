# Commiter

Middleware-пакет для [kafka-bus](https://github.com/kafka-bus/kafka-bus), обеспечивающий идемпотентную обработку сообщений: отслеживает обработанные сообщения, предотвращает дубликаты и ограничивает количество попыток обработки.

## Установка

```bash
composer require kafka-bus/commiter
```

## Архитектура

Пакет разделён на два слоя, и это разделение важно понимать при настройке:

```
ConsumerCommiterMiddleware
        │ работает с
        ▼
ConsumerMessageRepositoryInterface   ← как вычислить ключ из сообщения
        │  (IdempotencyMessageRepository | NativeMessageRepository)
        │ делегирует хранение
        ▼
RepositorySourceInterface            ← где физически хранить состояние
   (ArrayRepositorySource | DatabaseRepositorySource | RedisRepositorySource)
```

| Слой                                 | Отвечает за                                              |
|--------------------------------------|----------------------------------------------------------|
| `ConsumerMessageRepositoryInterface` | Как получить ключ дедупликации из сообщения Kafka        |
| `RepositorySourceInterface`          | Как хранить и обновлять состояние по этому ключу         |

### Attempt

Состояние одного ключа описывается неизменяемым объектом:

```php
namespace KafkaBus\Commiter;

final readonly class Attempt
{
    public function __construct(
        public string $key,
        public int $number = 1,
        public ?DateTimeImmutable $commitedAt = null,
    ) {}
}
```

| Поле         | Описание                                                              |
|--------------|-----------------------------------------------------------------------|
| `key`        | Ключ дедупликации                                                     |
| `number`     | Номер текущей попытки обработки                                       |
| `commitedAt` | Время успешной обработки; `null` — сообщение ещё не обработано        |

## Как это работает

`ConsumerCommiterMiddleware` встраивается в pipeline консьюмера и обрабатывает четыре сценария:

| Сценарий                                        | Действие                                              |
|-------------------------------------------------|-------------------------------------------------------|
| Сообщение уже обработано (`commitedAt != null`) | Логирует `warning`, останавливает pipeline            |
| Превышен `maxAttempt`                           | Логирует `error`, останавливает pipeline              |
| Сообщение обработано успешно                    | Вызывает `commit()`, логирует `debug`                 |
| Обработчик выбросил исключение                  | Вызывает `failed()`, перевыбрасывает исключение       |

В первых двух случаях pipeline возвращается **без** вызова `continue()` — обработчик не запускается, но offset всё равно коммитится, и сообщение больше не вернётся.

Во все записи лога добавляется контекст: `msg_id`, `topic_name`, `partition`, `offset`.

## Параметры middleware

```php
new ConsumerCommiterMiddleware(
    repository: $repository,       // ConsumerMessageRepositoryInterface
    logger:     new NullLogger(),  // PSR-3
    maxAttempt: 3,                 // -1 = безлимитно
);
```

| Параметр     | Тип                                  | По умолчанию | Описание                                 |
|--------------|--------------------------------------|--------------|------------------------------------------|
| `repository` | `ConsumerMessageRepositoryInterface` | —            | Стратегия вычисления ключа и хранения    |
| `logger`     | `LoggerInterface`                    | `NullLogger` | PSR-3 совместимый логгер                 |
| `maxAttempt` | `int`                                | `-1`         | Макс. попыток; `-1` или `0` — безлимитно |

::: warning
Первым аргументом идёт `ConsumerMessageRepositoryInterface` (`IdempotencyMessageRepository` или `NativeMessageRepository`), а **не** `RepositorySourceInterface`. Источник хранения передаётся внутрь репозитория.
:::

## Выбор репозитория

### По заголовку идемпотентности (рекомендуется)

Читает заголовок `x-idempotency-key` и строит ключ как `"{header}-{topicName}"`. Одинаковый ключ идемпотентности в разных топиках считается разными событиями. При отсутствии заголовка используется `msgId()`, поэтому старые продюсеры без идемпотентности продолжают работать.

```php
use KafkaBus\Commiter\Repositories\IdempotencyMessageRepository;

$repository = new IdempotencyMessageRepository($source);
```

### NativeMessageRepository

Использует `msgId()` — строку вида `"{partition}-{offset}-{topicName}"`.

## Выбор источника хранения

| Источник                   | Пакет                | Особенности                                       |
|----------------------------|----------------------|---------------------------------------------------|
| `ArrayRepositorySource`    | `kafka-bus/commiter` | В памяти процесса; для тестов                     |
| `DatabaseRepositorySource` | `laravel-bridge`     | Таблица `kafka_bus_commits`, поддерживает очистку |
| `RedisRepositorySource`    | `laravel-bridge`     | Хеши с TTL, атомарные Lua-скрипты                 |

::: tip
Вне Laravel реализуйте `RepositorySourceInterface` под своё хранилище — это три метода.
:::

## Базовое использование

### 1. Реализуйте источник хранения

```php
use DateTimeImmutable;
use KafkaBus\Commiter\Attempt;
use KafkaBus\Commiter\Interfaces\RepositorySourceInterface;

final class PdoRepositorySource implements RepositorySourceInterface
{
    public function __construct(private readonly PDO $pdo) {}

    /**
     * Возвращает текущее состояние ключа.
     * null означает, что сообщение ещё не встречалось.
     */
    public function get(string $key): ?Attempt
    {
        $stmt = $this->pdo->prepare('SELECT * FROM kafka_bus_commits WHERE `key` = ?');
        $stmt->execute([$key]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        if ($row === false) {
            return null;
        }

        return new Attempt(
            key: $row['key'],
            number: (int) $row['number'],
            commitedAt: $row['commited_at'] !== null
                ? new DateTimeImmutable($row['commited_at'])
                : null,
        );
    }

    /**
     * Увеличивает счётчик неудачных попыток, не помечая ключ обработанным.
     */
    public function increment(string $key): void
    {
        $this->pdo->prepare(
            'INSERT INTO kafka_bus_commits (`key`, number, commited_at) VALUES (?, 1, NULL)
             ON DUPLICATE KEY UPDATE number = number + 1'
        )->execute([$key]);
    }

    /**
     * Помечает ключ как успешно обработанный.
     */
    public function commit(string $key): void
    {
        $now = (new DateTimeImmutable())->format('Y-m-d H:i:s');

        $this->pdo->prepare(
            'INSERT INTO kafka_bus_commits (`key`, number, commited_at) VALUES (?, 1, ?)
             ON DUPLICATE KEY UPDATE number = number + 1, commited_at = VALUES(commited_at)'
        )->execute([$key, $now]);
    }
}
```

Контракт трёх методов:

| Метод              | Когда вызывается                       | Обязан                                            |
|--------------------|----------------------------------------|---------------------------------------------------|
| `get()`            | Перед обработкой                       | Вернуть `null`, если ключа нет                    |
| `increment()`      | После исключения в обработчике         | Увеличить `number`, оставить `commitedAt` пустым  |
| `commit()`         | После успешной обработки               | Проставить `commitedAt`                           |

::: warning
Реализуйте `increment()` и `commit()` атомарно (`upsert`, `INSERT ... ON DUPLICATE KEY UPDATE`, Lua-скрипт). Несколько консьюмеров одной группы могут обрабатывать сообщения параллельно, и вариант «прочитать, потом записать» приведёт к гонке.
:::

### 2. Подключите middleware

```php
use KafkaBus\Commiter\Middleware\ConsumerCommiterMiddleware;
use KafkaBus\Commiter\Repositories\IdempotencyMessageRepository;
use KafkaBus\Core\Bus\Listeners\Workers\MemoryWorkerRegistry;
use KafkaBus\Core\Bus\Listeners\Workers\Options;
use KafkaBus\Core\Bus\Listeners\Workers\Worker;

$repository = new IdempotencyMessageRepository(new PdoRepositorySource($pdo));

$workerRegistry = MemoryWorkerRegistry::make()
    ->add(new Worker(
        name:    'default',
        routes:  $consumerRoutes,
        options: new Options(
            middleware: [
                new ConsumerCommiterMiddleware(
                    repository: $repository,
                    logger:     $logger,
                    maxAttempt: 3,
                )
            ]
        )
    ));
```

::: tip Порядок middleware
Ставьте `ConsumerCommiterMiddleware` **первым** в списке. Тогда проверка дублей произойдёт до того, как отработают логирование, метрики и прочая обвязка.
:::

### Пример для тестов

```php
use KafkaBus\Commiter\Repositories\ArrayRepositorySource;

$source = new ArrayRepositorySource();
$repository = new IdempotencyMessageRepository($source);

// после прогона pipeline
assert($source->get('order-42-fact.orders.1')?->commitedAt !== null);
```

Состояние живёт только в пределах процесса — для продакшена не подходит.

## Ключи идемпотентности

По умолчанию ключ дедупликации — `msgId()`, комбинация партиции, offset и топика. Это работает, пока одно физическое сообщение не появляется с другим offset.

**Проблема:** при повторных отправках, ре-публикации или межкластерном зеркалировании одно логическое событие приходит с другим `msgId()` и обходит проверку.

**Решение:** ключ идемпотентности — стабильный идентификатор, который продюсер кладёт в заголовок `x-idempotency-key`. Консьюмер использует его вместо `msgId()`.

### Выбор правильного ключа

✅ Хорошие варианты:
- `order-42-v3` (aggregate id + версия)
- id строки в таблице outbox
- любое значение, которое апстрим считает уникальным для события

❌ Плохие варианты:
- timestamp (меняется при повторной отправке)
- случайный UUID, генерируемый при каждой отправке

## ProducerIdempotencyMiddleware

### Добавьте идемпотентность сообщению

```php
use KafkaBus\Commiter\Interfaces\HasIdempotency;
use KafkaBus\Core\Interfaces\Producers\Messages\ProducerMessageInterface;

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

### Подключите middleware к маршруту продюсера

```php
use KafkaBus\Commiter\Middleware\ProducerIdempotencyMiddleware;
use KafkaBus\Core\Bus\Publishers\Router\Options;
use KafkaBus\Core\Bus\Publishers\Router\PublisherRoutesBuilder;

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

Middleware добавляет заголовок `x-idempotency-key` к каждому исходящему сообщению. Если сообщение не реализует `HasIdempotency`, заголовок не добавляется, а сообщение публикуется как обычно.

## Ограничение попыток

`maxAttempt` защищает от «ядовитых» сообщений, которые падают при каждой обработке:

```php
new ConsumerCommiterMiddleware($repository, $logger, maxAttempt: 3);
```

Как это работает:

1. Обработчик падает → `failed()` увеличивает `number`, исключение всплывает и останавливает воркер;
2. Воркер перезапускается (Supervisor), сообщение читается снова с того же offset;
3. Когда `number` превысит `maxAttempt`, middleware залогирует `error` и пропустит сообщение, не вызывая обработчик;
4. Offset коммитится, воркер идёт дальше.

::: warning
`maxAttempt` не превращает воркер в отказоустойчивый сам по себе: между попытками процесс всё равно падает. Чтобы обрабатывать ошибки без перезапуска, добавьте middleware с `try/catch` — см. [Pipeline консьюмера](/ru/docs/1.x/consumer/pipeline#middleware-с-обработкои-ошибок). Но тогда `failed()` вызван не будет, и счётчик попыток расти перестанет.
:::

## Интеграция с Laravel

В `laravel-bridge` весь этот слой уже собран: источники (`database`, `redis`), выбор репозитория, миграция таблицы и команды `kafka:commit` / `kafka:commit:purge`. См. [Конфигурация Laravel](/ru/docs/1.x/laravel/configuration#commiter).
