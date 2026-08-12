# Команды

## Обзор команд

| Команда                                      | Описание                                              |
|----------------------------------------------|-------------------------------------------------------|
| `kafka:consume {worker*}`                    | Запуск долгоживущего процесса консьюмера              |
| `kafka:consume:all {--expect=*}`             | Запуск всех воркеров в одном процессе                 |
| `kafka:worker:list`                          | Список воркеров, топиков и обработчиков               |
| `kafka:route:list`                           | Список маршрутов продюсера                           |
| `kafka:offset:show {worker}`                 | Текущие offset воркера                                |
| `kafka:offset:set {worker} {topic} {offset}` | Установка offset                                      |
| `kafka:commit {key}`                         | Ручное создание записи коммита                        |
| `kafka:commit:purge {--days=}`               | Удаление устаревших записей коммитов                  |

Команды регистрируются только при запуске в консоли.

## kafka:consume

Запускает блокирующий цикл чтения для указанных воркеров. Процесс продолжается до получения `SIGTERM`, `SIGINT` или `SIGQUIT`.

```bash
php artisan kafka:consume products
php artisan kafka:consume default
php artisan kafka:consume products orders   # несколько воркеров в одном процессе
```

Команда реализует `SignalableCommandInterface`: по сигналу вызывается `forceStop()`, текущее сообщение дообрабатывается и коммитится, после чего процесс завершается корректно.

Коды возврата:

| Код | Ситуация                                                        |
|-----|-----------------------------------------------------------------|
| `0` | Цикл завершён штатно (`Consumer finished`)                      |
| `1` | Не передано ни одного воркера или произошло `ConsumerException` |

При ошибке сообщение пишется и в лог `LoggerInterface::error`, и в вывод команды.

::: warning
Исключения из обработчика `MessageConsumerNotHandledException` команда **не** перехватывает — они всплывают наружу и роняют процесс. Это осознанное поведение: перезапуск через Supervisor вернёт консьюмер к необработанному сообщению.
:::

::: tip
В продакшне оберните команду в Supervisor — см. [конфигурацию](/ru/docs/laravel/consumers#конфигурация-supervisor).
:::

## kafka:consume:all

Запускает все воркеры из `kafka-bus.consumers.workers` в одном процессе.

```bash
php artisan kafka:consume:all

# Исключить отдельные воркеры
php artisan kafka:consume:all --expect=products-analytics --expect=legacy
```

::: warning
Все воркеры объединяются в один консьюмер, а настройки подключения берутся у первого. Если у воркеров разные `group.id` или режимы коммита, применится только конфигурация первого — запускайте такие воркеры отдельными процессами.
:::

## kafka:worker:list

Выводит полный список зарегистрированных воркеров с их конфигурацией.

```bash
php artisan kafka:worker:list
```

```
+----------+-----------+----------------------------+------------------------------------------+---------------------+------------------+
| Worker   | Topic key | Topic name                 | Handler                                  | Consumer Middleware | Route Middleware |
+----------+-----------+----------------------------+------------------------------------------+---------------------+------------------+
| default  | products  | production.fact.products.1 | App\Kafka\Consumers\ProductsConsumer     |                     |                  |
| default  | orders    | production.fact.orders.1   | App\Kafka\Consumers\OrdersConsumer       | ConsumerCommiter... | TenantMiddleware |
| products | products  | production.fact.products.1 | App\Kafka\Consumers\ProductsConsumer     |                     |                  |
+----------+-----------+----------------------------+------------------------------------------+---------------------+------------------+
```

Одна строка на пару «воркер + топик». Команда резолвит обработчики и middleware через контейнер, поэтому заодно проверяет, что все классы существуют и создаются. Если воркеров нет, выводится предупреждение `No workers registered`.

## kafka:route:list

Выводит зарегистрированные маршруты продюсера.

```bash
php artisan kafka:route:list
```

```
+------------------------------------------+-----------+----------------------------+---------------------------------+
| Message                                  | Topic key | Topic name                 | Middleware                      |
+------------------------------------------+-----------+----------------------------+---------------------------------+
| App\Kafka\Messages\ProductMessage         | products  | production.fact.products.1 |                                 |
| App\Kafka\Messages\OrderMessage           | orders    | production.fact.orders.1   | ProducerIdempotencyMiddleware   |
+------------------------------------------+-----------+----------------------------+---------------------------------+
```

## kafka:offset:show

Показывает текущий, минимальный и максимальный offset для каждой партиции каждого топика воркера.

```bash
php artisan kafka:offset:show products
```

```
+-----------+----------------------------+-----------+---------+-----+-----+
| Topic key | Topic name                 | Partition | Current | Min | Max |
+-----------+----------------------------+-----------+---------+-----+-----+
| products  | production.fact.products.1 | 0         | 142     | 0   | 200 |
| products  | production.fact.products.1 | 1         | 90      | 0   | 150 |
+-----------+----------------------------+-----------+---------+-----+-----+
```

Удобно для мониторинга отставания консьюмера. Отставание = `Max - Current`.

::: tip
Партиция `n/a` со значениями `-1` означает, что топик ещё не создан в кластере или не имеет метаданных. Это нормально для нового окружения.
:::

## kafka:offset:set

Устанавливает закоммиченный offset для топика воркера. Позволяет перечитать сообщения или пропустить накопившийся бэклог.

```bash
kafka:offset:set {workerName} {topicKey} {offset} {--partition=}
```

Аргумент `offset` принимает:

| Значение                          | Действие                          |
|-----------------------------------|-----------------------------------|
| `earliest`, `early`, `beginning`  | Минимальный доступный offset      |
| `latest`, `end`                   | Максимальный доступный offset     |
| Целое число ≥ 0                   | Конкретная позиция                |

Регистр не важен. Любое другое значение вызовет ошибку с подсказкой.

### Перейти к первому сообщению (перечитать всё)

```bash
php artisan kafka:offset:set products products earliest
```

### Перейти к последнему сообщению (пропустить бэклог)

```bash
php artisan kafka:offset:set products products latest
```

### Установить конкретный оффсет

```bash
php artisan kafka:offset:set products products 150
```

### Установить оффсет для конкретной партиции

```bash
php artisan kafka:offset:set products products 150 --partition=0
```

Без `--partition` изменение применяется ко всем партициям топика.

Команда выводит результат:

```
+-----------+----------------------------+-----------+-----+-----+
| Topic key | Topic name                 | Partition | Old | New |
+-----------+----------------------------+-----------+-----+-----+
| products  | production.fact.products.1 | 0         | 142 | 0   |
| products  | production.fact.products.1 | 1         | 90  | 0   |
+-----------+----------------------------+-----------+-----+-----+
```

Команда завершится с кодом `1` и понятным сообщением, если:

- ключ топика не зарегистрирован (`TopicCannotResolvedException`);
- топик не принадлежит воркеру или указанная партиция не найдена;
- числовой offset выходит за границы `[Min, Max]`;
- воркер не зарегистрирован (`ListenerException`).

::: danger Важно
Воркер должен быть **остановлен** во время сброса offset. Если процесс консьюмера запущен, он перезапишет новую позицию при следующем коммите.
:::

## kafka:commit

Вручную создаёт запись коммита в хранилище идемпотентности для указанного ключа. Полезно, когда сообщение нужно пометить как обработанное без прохождения через стандартный pipeline консьюмера.

```bash
php artisan kafka:commit {key}
```

### Пример

```bash
php artisan kafka:commit order-12345
```

Если коммит для данного ключа уже существует, команда выведет предупреждение и завершится с кодом `1`:

```
 WARN  Commit for key [order-12345] already exists.
```

В противном случае коммит будет сохранён:

```
 INFO  Commit for key [order-12345] has been saved.
```

::: warning Ключ должен совпадать с тем, что вычислит консьюмер
Команда работает с сырым ключом. При активном `IdempotencyMessageRepository` реальный ключ выглядит как `"{x-idempotency-key}-{полное имя топика}"`, например `order-12345-production.fact.orders.1`, а без заголовка — как `"{partition}-{offset}-{имя топика}"`.

Подсмотреть точный ключ можно в логах `ConsumerCommiterMiddleware` — он пишется в поле `msg_id`.
:::

::: tip
Команда предназначена для сценариев восстановления — например, когда сообщение было обработано вне консьюмера или его нужно навсегда пропустить.
:::

## kafka:commit:purge

Удаляет записи коммитов старше заданного срока — иначе таблица растёт бесконечно.

```bash
php artisan kafka:commit:purge
php artisan kafka:commit:purge --days=30
```

Без `--days` берётся значение из `kafka-bus-commiter.purge.days` (по умолчанию `14`).

```
 INFO  Purged 1523 commit(s) older than 14 day(s).
```

Удаляются только записи с заполненным `commited_at` — незавершённые попытки сохраняются.

::: warning Только для драйвера `database`
Команда требует, чтобы активный источник реализовывал `PurgeInterface`. Для источника `redis` она завершится с кодом `1` и сообщением `The active repository source does not support purging` — там очистка выполняется через TTL ключей.
:::

Команду удобно повесить на расписание:

```php
// routes/console.php
Schedule::command('kafka:commit:purge')->daily();
```
