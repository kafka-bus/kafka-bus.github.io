# Artisan команды

## Обзор команд

| Команда                                              | Описание                                           |
|------------------------------------------------------|----------------------------------------------------|
| `kafka:consume {worker}`                             | Запуск долгоживущего процесса консьюмера           |
| `kafka:worker:list`                                  | Список воркеров, топиков и обработчиков               |
| `kafka:route:list`                                   | Список маршрутов продьюсера                        |
| `kafka:offset:show {worker}`                         | Текущие offset воркера                             |
| `kafka:offset:set {worker} {topic} {offset}`         | Установка offset                                   |
| `kafka:commit {key}`                                 | Ручное создание записи коммита в базе данных       |

## kafka:consume

Запускает блокирующий цикл чтения для указанного воркера. Процесс продолжается до получения `SIGINT` или `SIGTERM`.

```bash
php artisan kafka:consume products
php artisan kafka:consume default
php artisan kafka:consume products-secondary
```

::: tip
В продакшне оберните команду в Supervisor для автоматического перезапуска при сбоях.
:::

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
