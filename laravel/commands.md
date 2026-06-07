# Artisan-команды

## Обзор команд

| Команда | Описание |
|---|---|
| `kafka:consume {worker}` | Запуск длительного consumer-процесса |
| `kafka:worker:list` | Список воркеров, топиков и обработчиков |
| `kafka:route:list` | Список маршрутов producer'а |
| `kafka:offset:show {worker}` | Текущие офсеты воркера |
| `kafka:offset:set {worker} {topic} {offset}` | Установка офсета |

---

## kafka:consume

Запускает блокирующий цикл чтения для указанного воркера. Процесс продолжается до получения `SIGINT` или `SIGTERM`.

```bash
php artisan kafka:consume products
php artisan kafka:consume default
php artisan kafka:consume products-secondary
```

::: tip
Для продакшна оберните команду в Supervisor, чтобы обеспечить автоматический перезапуск при падении.
:::

---

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

Используйте для быстрой проверки, что все воркеры настроены корректно после изменения конфига.

---

## kafka:route:list

Выводит зарегистрированные маршруты producer'а.

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

---

## kafka:offset:show

Показывает текущий, минимальный и максимальный офсеты для каждой партиции каждого топика воркера.

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

Полезно для мониторинга лага consumer'а. Лаг = `Max - Current`.

---

## kafka:offset:set

Устанавливает committed offset для топика воркера. Позволяет перечитать сообщения или пропустить накопившийся бэклог.

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

### Установить конкретный офсет

```bash
php artisan kafka:offset:set products products 150
```

### Установить офсет для конкретной партиции

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
Воркер должен быть **остановлен** во время сброса офсетов. Если consumer-процесс работает, он перезапишет новую позицию при следующем коммите.
:::
