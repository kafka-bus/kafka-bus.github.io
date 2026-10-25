---
title: Metadata
description: Список топиков и партиций, оффсеты групп консьюмеров и сброс оффсетов с помощью kafka-bus/metadata
---

# Metadata

Пакет `kafka-bus/metadata` читает метаданные кластера: какие топики и партиции существуют, где сейчас находится группа консьюмеров в каждой партиции и насколько она отстаёт. Он также позволяет сдвигать оффсеты группы — например, чтобы перечитать топик с начала или пропустить накопившийся хвост.

Пакет не зависит от воркеров и конкретного консьюмера: топики и группа консьюмеров передаются явно.

## Установка

```bash
composer require kafka-bus/metadata
```

Пакет уже входит в мета-пакет `kafka-bus/kafka-bus`.

## Точка входа

`Metadata` создаётся из подключения. Он использует только параметры librdkafka подключения — список брокеров, SASL и т. д. — и открывает собственные короткоживущие продюсер и консьюмер для запросов метаданных:

```php
use KafkaBus\Metadata\Metadata;

$metadata = Metadata::fromConnection($connectionRegistry->connection('default'));
```

| Метод | Возвращает | Описание |
|---|---|---|
| `topics()` | `TopicsMetadata` | Топики и партиции всего кластера |
| `consumerGroup(ConsumerConfig $config)` | `ConsumerGroupMetadata` | Сырые оффсеты одной группы консьюмеров |
| `partitions(array $topics, ConsumerConfig $config)` | `Partitions` | Оффсеты группы по вашим топикам с безопасной сменой оффсета |

## Список топиков

`topics()->list()` возвращает все топики, известные брокеру. Передайте список объектов `Topic`, чтобы ограничить результат:

```php
foreach ($metadata->topics()->list($topicRegistry->all()) as $topic) {
    foreach ($topic->partitions as $partition) {
        echo "{$topic->name}#{$partition->id} [{$partition->offset}]\n";
    }
}
```

У `TopicMetadata` есть `name` и `partitions` (`list<PartitionMetadata>`); у каждого `PartitionMetadata` — `id` и `offset`.

Чтобы получить один топик по физическому имени, используйте `get()`. Если топика нет, он бросает `TopicNotFoundException`:

```php
$topic = $metadata->topics()->get('production.fact.products.1');
```

## Оффсеты группы консьюмеров

`partitions()` объединяет метаданные кластера с закоммиченными оффсетами группы консьюмеров. Группа задаётся параметром `group.id` в `ConsumerConfig`:

```php
use KafkaBus\Core\Consumers\ConsumerConfig;

$partitions = $metadata->partitions(
    topics: [$topicRegistry->get('products')],
    config: new ConsumerConfig(additionalOptions: ['group.id' => 'products-service']),
);

foreach ($partitions->list() as $partition) {
    $lag = $partition->maxOffset - $partition->currentOffset;

    echo "{$partition->topic->key}#{$partition->id}: "
        . "{$partition->currentOffset} / [{$partition->minOffset}, {$partition->maxOffset}], lag {$lag}\n";
}
```

У каждого `TopicPartition` есть:

| Свойство | Тип | Описание |
|---|---|---|
| `id` | `int` | Номер партиции; `-1`, если топика нет в кластере |
| `topic` | `Topic` | Топик из вашего реестра |
| `currentOffset` | `int` | Закоммиченный оффсет группы; отрицательный, если группа ещё ничего не коммитила |
| `minOffset` | `int` | Нижняя граница — самый старый доступный оффсет |
| `maxOffset` | `int` | Верхняя граница — оффсет следующего записываемого сообщения |

Несуществующий топик не ломает список: для него возвращается одна строка с `id = -1` и оффсетами `-1`.

## Установка оффсетов

`setOffset()` сдвигает закоммиченный оффсет группы для одной партиции или для всех партиций топика:

```php
use KafkaBus\Metadata\Partitions\CommitOffset;
use KafkaBus\Metadata\Partitions\Offset;

$results = $partitions->setOffset(new CommitOffset(
    topic: $topicRegistry->get('products'),
    partition: RD_KAFKA_PARTITION_UA, // все партиции
    offset: Offset::Early,            // или Offset::Latest, или число
));

foreach ($results as $result) {
    echo "{$result->topic->name}#{$result->partition}: {$result->oldOffset} → {$result->newOffset}\n";
}
```

Параметры `CommitOffset`:

| Параметр | Тип | Описание |
|---|---|---|
| `topic` | `Topic` | Должен быть среди топиков, переданных в `partitions()` |
| `partition` | `int` | Номер партиции или `RD_KAFKA_PARTITION_UA` для всех партиций |
| `offset` | `Offset\|int` | `Offset::Early` — нижняя граница, `Offset::Latest` — верхняя, или конкретный оффсет |

`setOffset()` возвращает `list<CommitOffsetResult>` с полями `topic`, `partition`, `oldOffset` и `newOffset` для каждой изменённой партиции.

Он бросает `CannotCommitOffsetException`, если:

- топик не передавался в `partitions()`;
- топика нет в кластере;
- партиции не существует;
- оффсет вне диапазона `[minOffset, maxOffset]`.

::: danger Сначала остановите консьюмеры
Перед сменой оффсетов остановите все воркеры группы консьюмеров. Работающий консьюмер перезапишет новую позицию следующим коммитом.
:::

## Прямой доступ к группе консьюмеров

`consumerGroup()` возвращает низкоуровневый `ConsumerGroupMetadata`, на котором построен `Partitions`. Он работает с физическими именами топиков и ничего не проверяет:

```php
use KafkaBus\Metadata\ConsumerGroups\PartitionOffset;

$group = $metadata->consumerGroup(new ConsumerConfig(additionalOptions: ['group.id' => 'products-service']));

$group->partitions('production.fact.products.1'); // list<ConsumerPartition>
$group->commit([new PartitionOffset('production.fact.products.1', 0, 150)]);
```

Предпочитайте `partitions()`, если не нужно обойти проверку границ.

## Laravel

Laravel-бридж открывает этот пакет через команды `kafka:consumer:offset:show` и `kafka:consumer:offset:set`. См. [Команды](/ru/docs/2.x/laravel/commands).
