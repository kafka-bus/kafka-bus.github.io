# kafka-bus-outbox

Реализация паттерна **Transactional Outbox** для надёжной публикации сообщений в Kafka в связке с транзакциями базы данных.

## Проблема, которую решает Outbox

При стандартной публикации через `Bus::publish()` существует риск частичного сбоя:

```
// Сценарий без Outbox
DB::transaction(function () use ($bus) {
    $order = Order::create($data);     // ✅ сохранено в БД
    $bus->publish(new OrderCreated()); // ❌ Kafka упала — сообщение потеряно
    // Транзакция откатилась, но данные в БД уже есть
});
```

Паттерн Outbox гарантирует, что сообщение будет доставлено ровно один раз, даже если Kafka недоступна в момент транзакции.

## Как работает паттерн

```
DB::transaction(function () {
    Order::create($data);         // бизнес-запись
    Outbox::store(new OrderCreated()); // запись в ту же БД — атомарно
});

// Отдельный процесс — Outbox relay
Outbox::dispatch(); // читает необработанные записи → публикует в Kafka → помечает как отправленные
```

Обе записи (заказ и outbox-событие) живут в одной транзакции. Если транзакция откатывается — откатываются обе. Relay публикует только зафиксированные записи.

## Установка

```bash
composer require micromus/kafka-bus-outbox
```

::: tip
Документация этого раздела находится в разработке. Актуальная информация — в [репозитории пакета](https://github.com/micromus/kafka-bus-outbox).
:::

## Ссылки

- [GitHub: kafka-bus-outbox](https://github.com/micromus/kafka-bus-outbox)
- [Статья: Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)
