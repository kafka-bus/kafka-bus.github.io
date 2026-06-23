# Команды

## Обзор команд

| Команда                                              | Описание                                           |
|------------------------------------------------------|----------------------------------------------------|
| `kafka:consume {worker*}`                            | Запуск долгоживущего процесса консьюмера           |
| `kafka:consume:all {--expect=*}`                     | Запуск консьюмеров для всех настроенных воркеров   |
| `kafka:worker:list`                                  | Список воркеров, топиков и обработчиков            |
| `kafka:route:list`                                   | Список маршрутов продьюсера                        |
| `kafka:offset:show {worker}`                         | Текущие offset воркера                             |
| `kafka:offset:set {worker} {topic} {offset}`         | Установка offset                                   |
| `kafka:commit {key}`                                 | Ручное создание записи коммита в базе данных       |
| `kafka:commit:purge {--days=}`                      | Удаление устаревших записей коммитов               |

## kafka:consume

::: info Доступно с `kafka-bus/laravel-bridge:^1.2`
Поддержка нескольких имён воркеров добавлена в версии 1.2.
:::

Запускает блокирующий цикл чтения для одного или нескольких воркеров. Процесс продолжается до получения `SIGINT` или `SIGTERM`.

```bash
# Один воркер
php artisan kafka:consume products

# Несколько воркеров в одном процессе
php artisan kafka:consume products orders
php artisan kafka:consume products orders notifications
```

При передаче нескольких имён воркеров их топики объединяются в единый consumer stream.

::: tip
В продакшне оберните команду в Supervisor для автоматического перезапуска при сбоях.
:::

## kafka:consume:all

::: info Доступно с `kafka-bus/laravel-bridge:^1.2`
:::

Запускает консьюмеры для **всех** воркеров, объявленных в `config/kafka-bus.php` в секции `consumers.workers`, в едином процессе. Используйте `--expect` для исключения конкретных воркеров.

```bash
# Запустить все воркеры
php artisan kafka:consume:all

# Запустить все воркеры кроме "orders"
php artisan kafka:consume:all --expect=orders

# Исключить несколько воркеров
php artisan kafka:consume:all --expect=orders --expect=notifications
```

## kafka:worker:list

Выводит полный список зарегистрированных воркеров с их конфигурацией.

```bash
php artisan kafka:worker:list
```

```
+----------+-----------+----------------------------+------------------------------------------+---------------------+------------------+
| Worker   | Topic key | Topic name                 | Handler                                  | Consumer Middleware | Route Middleware  |
+----------+-----------+----------------------------+------------------------------------------+---------------------+------------------+
| default  | products  | production.fact.products.1 | App\Kafka\Consumers\ProductsConsumer     |                     |                  |
| default  | orders    | production.fact.orders.1   | App\Kafka\Consumers\OrdersConsumer       | ConsumerCommiter... | TenantMiddleware |
| products | products  | production.fact.products.1 | App\Kafka\Consumers\ProductsConsumer     |                     |                  |
+----------+-----------+----------------------------+------------------------------------------+---------------------+------------------+
```

Используйте для быстрой проверки корректности конфигурации воркеров после изменений.

## kafka:route:list

Выводит зарегистрированные маршруты продьюсера.

```bash
php artisan kafka:route:list
```

```
+------------------------------------------+-----------+----------------------------+------------+
| Message                                  | Topic key | Topic name                 | Middleware |
+------------------------------------------+-----------+----------------------------+------------+
| App\Kafka\Messages\ProductMessage        | products  | production.fact.products.1 | 0          |
| App\Kafka\Messages\OrderMessage          | orders    | production.fact.orders.1   | 1          |
+------------------------------------------+-----------+----------------------------+------------+
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

## kafka:offset:set

Устанавливает закоммиченный offset для топика воркера. Позволяет перечитать сообщения или пропустить накопившийся бэклог.

```bash
kafka:offset:set {workerName} {topicKey} {offset} {--partition=}
```

### Перейти к первому сообщению (перечитать всё)

```bash
php artisan kafka:offset:set products products earliest
```

### Перейти к последнему сообщению (пропустить бэклог)

```bash
php artisan kafka:offset:set products products latest
```

### Установить конкретный offset

```bash
php artisan kafka:offset:set products products 150
```

### Установить offset для конкретной партиции

```bash
php artisan kafka:offset:set products products 150 --partition=0
```

Команда выводит результат:

```
+-----------+----------------------------+-----------+-----+-----+
| Topic key | Topic name                 | Partition | Old | New |
+-----------+----------------------------+-----------+-----+-----+
| products  | production.fact.products.1 | 0         | 142 | 0   |
| products  | production.fact.products.1 | 1         | 90  | 0   |
+-----------+----------------------------+-----------+-----+-----+
```

::: danger Важно
Воркер должен быть **остановлен** во время сброса offset. Если процесс консьюмера запущен, он перезапишет новую позицию при следующем коммите.
:::

## kafka:commit

Вручную создаёт запись коммита в базе данных для указанного ключа. Полезно, когда сообщение нужно пометить как обработанное без прохождения через стандартный pipeline консьюмера.

```bash
php artisan kafka:commit {key}
```

### Пример

```bash
php artisan kafka:commit order-12345
```

Если коммит для данного ключа уже существует, команда выведет предупреждение и завершится с ненулевым кодом:

```
 WARN  Commit for key [order-12345] already exists.
```

В противном случае коммит будет сохранён и команда сообщит об успехе:

```
 INFO  Commit for key [order-12345] has been saved.
```

::: tip
Эта команда предназначена для сценариев восстановления — например, когда сообщение было обработано вне консьюмера или его нужно постоянно пропускать.
:::

## kafka:commit:purge

Удаляет из базы данных записи коммитов, которые старше настроенного срока хранения. Позволяет предотвратить неограниченный рост таблицы `kafka_bus_commits` в долгоживущих приложениях.

```bash
php artisan kafka:commit:purge
```

По умолчанию срок хранения берётся из `kafka-bus-commiter.purge.days` (по умолчанию: `14`). Можно переопределить для конкретного запуска через `--days`:

```bash
# Удалить записи старше 30 дней
php artisan kafka:commit:purge --days=30
```

Команда выводит количество удалённых записей:

```
 INFO  Purged 42 commit(s) older than 14 day(s).
```

::: info Redis-источник
Redis-источник не поддерживает эту команду — он управляет сроком хранения автоматически через TTL ключа, устанавливаемый при записи. Запуск `kafka:commit:purge` с Redis-источником завершится с ошибкой:

```
The active repository source does not support purging.
```
:::

::: tip Планировщик
Добавьте команду в планировщик, чтобы старые записи очищались автоматически:

```php
// routes/console.php  (Laravel 11+)
Schedule::command('kafka:commit:purge')->daily();
```
:::
