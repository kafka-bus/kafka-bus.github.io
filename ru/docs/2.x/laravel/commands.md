---
title: Artisan-команды
description: Справочник Artisan-команд Kafka Bus — запуск воркеров, просмотр маршрутов, управление оффсетами и записями коммитов
---

# Команды

Laravel-бридж регистрирует Artisan-команды для запуска воркеров, просмотра маршрутов, управления оффсетами групп консьюмеров и обслуживания хранилища Commiter. Команды регистрируются только при запуске приложения в консоли.

## Обзор команд

| Команда | Описание |
|---|---|
| `kafka:consume {workerName*}` | Запускает один или несколько воркеров в одном процессе |
| `kafka:consume:routes {--expect=*}` | Запускает все маршруты консьюмера в одном процессе |
| `kafka:worker:list` | Список воркеров и их топиков |
| `kafka:consumer:route:list` | Маршруты консьюмера: топик → обработчик и middleware |
| `kafka:publisher:route:list` | Маршруты продюсера: класс сообщения → топик |
| `kafka:consumer:offset:show {topicKey?}` | Показывает оффсеты группы консьюмеров |
| `kafka:consumer:offset:set {topicKey} {offset} {--partition=}` | Устанавливает оффсеты группы консьюмеров |
| `kafka:commit {key}` | Вручную создаёт запись коммита |
| `kafka:commit:purge {--days=}` | Удаляет устаревшие записи коммитов |

::: info Изменено в 2.0
`kafka:consume:all` → `kafka:consume:routes`, `kafka:route:list` → `kafka:publisher:route:list`, `kafka:offset:*` → `kafka:consumer:offset:*` без аргумента воркера. `kafka:consumer:route:list` — новая команда. См. [Переход с 1.x](/ru/docs/2.x/upgrade#artisan-команды).
:::

## kafka:consume

Запускает блокирующий цикл чтения для указанных воркеров. Процесс работает, пока не получит `SIGTERM`, `SIGINT` или `SIGQUIT`.

```bash
php artisan kafka:consume products
php artisan kafka:consume default
php artisan kafka:consume products orders   # несколько воркеров в одном процессе
```

Имя воркера — ключ из `consumers.workers` или ключ топика из `consumers.routes`. При нескольких именах их топики объединяются в один консьюмер, а параметры берутся у первого воркера.

По сигналу команда вызывает `forceStop()`: текущее сообщение дообрабатывается и коммитится, затем процесс завершается.

Коды выхода:

| Код | Ситуация |
|---|---|
| `0` | Цикл завершился штатно (`Consumer finished`) |
| `1` | Не передано имя воркера или возникло `ConsumerException` |

При `ConsumerException` сообщение пишется и в `LoggerInterface::error`, и в вывод команды.

::: warning
Команда не перехватывает `MessageConsumerNotHandledException` из обработчиков — исключение пробрасывается и останавливает процесс. Это сделано намеренно: после перезапуска через Supervisor воркер продолжит с необработанного сообщения.
:::

::: tip
В продакшене запускайте команду под Supervisor — см. [конфигурацию](/ru/docs/2.x/laravel/consumers#конфигурация-supervisor).
:::

## kafka:consume:routes

Запускает все топики из `consumers.routes` в одном процессе с глобальными параметрами консьюмера. Объявленные воркеры игнорируются.

```bash
php artisan kafka:consume:routes

# Исключить топики
php artisan kafka:consume:routes --expect=orders --expect=legacy
```

`--expect` принимает ключи топиков. Если читать нечего, команда выводит `No workers for consuming.` и завершается с кодом `1`.

## kafka:worker:list

Выводит все воркеры — объявленные в `consumers.workers` и производные от маршрутов — с их топиками и обработчиками.

```bash
php artisan kafka:worker:list
```

```
+----------+-----------+----------------------------+--------------------------------------+
| Worker   | Topic key | Topic name                 | Handler                              |
+----------+-----------+----------------------------+--------------------------------------+
| default  | products  | production.fact.products.1 | App\Kafka\Consumers\ProductsConsumer |
| default  | orders    | production.fact.orders.1   | App\Kafka\Consumers\OrdersConsumer   |
| products | products  | production.fact.products.1 | App\Kafka\Consumers\ProductsConsumer |
| orders   | orders    | production.fact.orders.1   | App\Kafka\Consumers\OrdersConsumer   |
+----------+-----------+----------------------------+--------------------------------------+
```

Одна строка — одна пара «воркер + топик». Обработчик берётся из `consumers.routes`; `?` означает, что у топика нет маршрута. Если воркеров нет, команда выводит `No workers registered`.

## kafka:consumer:route:list

Выводит маршруты консьюмера так, как их видит шина: топик, обработчик и итоговую цепочку middleware (глобальные + маршрута).

```bash
php artisan kafka:consumer:route:list
```

```
+-----------+----------------------------+--------------------------------------+----------------------------+
| Topic key | Topic name                 | Handler                              | Middleware                 |
+-----------+----------------------------+--------------------------------------+----------------------------+
| products  | production.fact.products.1 | App\Kafka\Consumers\ProductsConsumer | ConsumerCommiterMiddleware |
| orders    | production.fact.orders.1   | App\Kafka\Consumers\OrdersConsumer   | ConsumerCommiterMiddleware |
|           |                            |                                      | TenantMiddleware           |
+-----------+----------------------------+--------------------------------------+----------------------------+
```

Команда резолвит обработчики и middleware через контейнер, поэтому заодно проверяет, что все классы существуют и создаются. Middleware выводятся полными именами классов.

## kafka:publisher:route:list

Выводит маршруты продюсера.

```bash
php artisan kafka:publisher:route:list
```

```
+-----------------------------------+-----------+----------------------------+--------------------------------+
| Message                           | Topic key | Topic name                 | Middleware                     |
+-----------------------------------+-----------+----------------------------+--------------------------------+
| App\Kafka\Messages\ProductMessage | products  | production.fact.products.1 |                                |
| App\Kafka\Messages\OrderMessage   | orders    | production.fact.orders.1   | PublisherIdempotencyMiddleware |
+-----------------------------------+-----------+----------------------------+--------------------------------+
```

## kafka:consumer:offset:show

Показывает текущий, минимальный и максимальный оффсет каждой партиции читаемых топиков. Группа консьюмеров берётся из глобального `consumers.additional_options.group.id`.

```bash
# Все топики из consumers.routes
php artisan kafka:consumer:offset:show

# Один топик
php artisan kafka:consumer:offset:show products
```

```
+-----------+----------------------------+-----------+---------+-----+-----+
| Topic key | Topic name                 | Partition | Current | Min | Max |
+-----------+----------------------------+-----------+---------+-----+-----+
| products  | production.fact.products.1 | 0         | 142     | 0   | 200 |
| products  | production.fact.products.1 | 1         | 90      | 0   | 150 |
+-----------+----------------------------+-----------+---------+-----+-----+
```

Удобно для мониторинга отставания консьюмера: отставание = `Max - Current`.

::: tip
Партиция `n/a` со значениями `-1` означает, что топика ещё нет в кластере. Для свежего окружения это нормально.
:::

::: warning
Оффсеты показываются для глобальной группы консьюмеров. Воркер с собственным `group.id` в `additional_options` эти команды не покрывают — для него используйте [Metadata](/ru/docs/2.x/components/metadata) напрямую.
:::

## kafka:consumer:offset:set

Устанавливает закоммиченный оффсет глобальной группы консьюмеров для топика. Позволяет перечитать сообщения или пропустить накопившийся хвост.

```bash
php artisan kafka:consumer:offset:set {topicKey} {offset} {--partition=}
```

Аргумент `offset` принимает:

| Значение | Действие |
|---|---|
| `earliest`, `early`, `beginning` | Минимальный доступный оффсет |
| `latest`, `end` | Максимальный доступный оффсет |
| Целое число ≥ 0 | Конкретная позиция |

Регистр не важен. На любое другое значение команда выводит ошибку с подсказкой.

### Перечитать с начала

```bash
php artisan kafka:consumer:offset:set products earliest
```

### Пропустить накопившийся хвост

```bash
php artisan kafka:consumer:offset:set products latest
```

### Конкретный оффсет для одной партиции

```bash
php artisan kafka:consumer:offset:set products 150 --partition=0
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

Команда завершается с кодом `1` и сообщением, если:

- ключ топика не зарегистрирован (`TopicCannotResolvedException`);
- топика нет в кластере или указанной партиции не существует;
- числовой оффсет вне диапазона `[Min, Max]`.

::: danger Сначала остановите воркеры
Перед сменой оффсетов остановите все воркеры группы консьюмеров. Работающий консьюмер перезапишет новую позицию следующим коммитом.
:::

## kafka:commit

Создаёт запись коммита в хранилище Commiter для ключа. Полезно, когда сообщение нужно пометить обработанным, не запуская его обработчик.

```bash
php artisan kafka:commit order-12345
```

Если коммит для ключа уже есть, команда выводит предупреждение и завершается с кодом `1`:

```
 WARN  Commit for key [order-12345] already exists.
```

Иначе коммит сохраняется:

```
 INFO  Commit for key [order-12345] has been saved.
```

::: warning Ключ должен совпадать с тем, что вычисляет консьюмер
Команда работает с сырым ключом. С `IdempotencyMessageRepository` настоящий ключ выглядит как `"{x-idempotency-key}-{полное имя топика}"`, например `order-12345-production.fact.orders.1`; без заголовка — `"{partition}-{offset}-{имя топика}"`.

Точный ключ есть в логах `ConsumerCommiterMiddleware`, в поле `msg_id`.
:::

## kafka:commit:purge

Удаляет записи коммитов старше заданного возраста — иначе таблица растёт бесконечно.

```bash
php artisan kafka:commit:purge
php artisan kafka:commit:purge --days=30
```

Без `--days` значение берётся из `kafka-bus-commiter.purge.days` (по умолчанию `14`).

```
 INFO  Purged 1523 commit(s) older than 14 day(s).
```

::: warning Только для драйвера database
Команде нужно, чтобы активный источник реализовывал `PurgeInterface`. Для источника `redis` она завершается с кодом `1` и сообщением `The active repository source does not support purging` — там ключи истекают по TTL.
:::

Запускайте её по расписанию:

```php
// routes/console.php
Schedule::command('kafka:commit:purge')->daily();
```
