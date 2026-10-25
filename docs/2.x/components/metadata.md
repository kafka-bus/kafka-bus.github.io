---
title: Metadata
description: Listing topics and partitions, inspecting consumer group offsets, and resetting offsets with kafka-bus/metadata
---

# Metadata

The `kafka-bus/metadata` package reads cluster metadata: which topics and partitions exist, where a consumer group currently is in each partition, and how far behind it lags. It also lets you move a consumer group's offsets — for example, to re-read a topic from the beginning or to skip a backlog.

The package doesn't depend on workers or any particular consumer: you pass the topics and the consumer group explicitly.

## Installation

```bash
composer require kafka-bus/metadata
```

The package is already included in the `kafka-bus/kafka-bus` meta-package.

## Entry point

`Metadata` is created from a connection. It uses only the connection's librdkafka options — broker list, SASL, and so on — and opens its own short-lived producer and consumer for metadata requests:

```php
use KafkaBus\Metadata\Metadata;

$metadata = Metadata::fromConnection($connectionRegistry->connection('default'));
```

| Method | Returns | Description |
|---|---|---|
| `topics()` | `TopicsMetadata` | Topics and partitions of the whole cluster |
| `consumerGroup(ConsumerConfig $config)` | `ConsumerGroupMetadata` | Raw offsets of one consumer group |
| `partitions(array $topics, ConsumerConfig $config)` | `Partitions` | Offsets of a group for your topics, with safe offset changes |

## Listing topics

`topics()->list()` returns every topic known to the broker. Pass a list of `Topic` objects to limit the result:

```php
foreach ($metadata->topics()->list($topicRegistry->all()) as $topic) {
    foreach ($topic->partitions as $partition) {
        echo "{$topic->name}#{$partition->id} [{$partition->offset}]\n";
    }
}
```

`TopicMetadata` has `name` and `partitions` (`list<PartitionMetadata>`); each `PartitionMetadata` has `id` and `offset`.

To fetch one topic by its physical name, use `get()`. It throws a `TopicNotFoundException` if the topic doesn't exist:

```php
$topic = $metadata->topics()->get('production.fact.products.1');
```

## Consumer group offsets

`partitions()` combines the cluster metadata with the committed offsets of a consumer group. The group is set by `group.id` in the `ConsumerConfig`:

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

Each `TopicPartition` has:

| Property | Type | Description |
|---|---|---|
| `id` | `int` | Partition number; `-1` if the topic doesn't exist in the cluster |
| `topic` | `Topic` | The topic from your registry |
| `currentOffset` | `int` | The group's committed offset; negative if the group hasn't committed yet |
| `minOffset` | `int` | Low watermark — the oldest available offset |
| `maxOffset` | `int` | High watermark — the offset of the next message to be written |

A topic that doesn't exist yet doesn't fail the listing: it yields one row with `id = -1` and `-1` offsets.

## Setting offsets

`setOffset()` moves the group's committed offset for one partition or for all partitions of a topic:

```php
use KafkaBus\Metadata\Partitions\CommitOffset;
use KafkaBus\Metadata\Partitions\Offset;

$results = $partitions->setOffset(new CommitOffset(
    topic: $topicRegistry->get('products'),
    partition: RD_KAFKA_PARTITION_UA, // all partitions
    offset: Offset::Early,            // or Offset::Latest, or a number
));

foreach ($results as $result) {
    echo "{$result->topic->name}#{$result->partition}: {$result->oldOffset} → {$result->newOffset}\n";
}
```

`CommitOffset` parameters:

| Parameter | Type | Description |
|---|---|---|
| `topic` | `Topic` | Must be one of the topics passed to `partitions()` |
| `partition` | `int` | Partition number, or `RD_KAFKA_PARTITION_UA` for all partitions |
| `offset` | `Offset\|int` | `Offset::Early` — the low watermark, `Offset::Latest` — the high watermark, or an exact offset |

`setOffset()` returns a `list<CommitOffsetResult>` with `topic`, `partition`, `oldOffset`, and `newOffset` for every changed partition.

It throws a `CannotCommitOffsetException` when:

- the topic wasn't passed to `partitions()`;
- the topic doesn't exist in the cluster;
- the partition doesn't exist;
- the offset is outside `[minOffset, maxOffset]`.

::: danger Stop the consumers first
Stop every worker of the consumer group before changing offsets. A running consumer overwrites the new position with its next commit.
:::

## Raw consumer group access

`consumerGroup()` returns the lower-level `ConsumerGroupMetadata` that `Partitions` is built on. It works with physical topic names and does no validation:

```php
use KafkaBus\Metadata\ConsumerGroups\PartitionOffset;

$group = $metadata->consumerGroup(new ConsumerConfig(additionalOptions: ['group.id' => 'products-service']));

$group->partitions('production.fact.products.1'); // list<ConsumerPartition>
$group->commit([new PartitionOffset('production.fact.products.1', 0, 150)]);
```

Prefer `partitions()` unless you need to bypass the bounds check.

## Laravel

The Laravel bridge exposes this package through the `kafka:consumer:offset:show` and `kafka:consumer:offset:set` commands. See [Commands](/docs/2.x/laravel/commands).
