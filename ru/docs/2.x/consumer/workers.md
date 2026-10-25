---
title: Воркеры
description: Чтение Kafka через kafka-bus/worker — описание воркеров, запуск цикла чтения, объединение воркеров, коммит оффсетов и корректная остановка
---

# Воркеры

Воркер — долгоживущий процесс, который читает сообщения из Kafka и передаёт каждое из них в `Bus::dispatch()`. Он знает только, какие топики читать и с какими параметрами консьюмера, — обработчики, маршрутизация и middleware настраиваются в шине. Воркеры находятся в пакете `kafka-bus/worker`.

```bash
composer require kafka-bus/worker
```

Пакет уже входит в мета-пакет `kafka-bus/kafka-bus`.

## Описание воркера

`Worker` — это имя, список топиков и параметры консьюмера. Зарегистрируйте воркеры в `MemoryWorkerRegistry`:

```php
use KafkaBus\Worker\Options;
use KafkaBus\Worker\Registry\MemoryWorkerRegistry;
use KafkaBus\Worker\Worker;

$workerRegistry = MemoryWorkerRegistry::make()
    ->add(new Worker(
        name: 'products',
        topics: [$topicRegistry->get('products')],
        options: new Options(
            additionalOptions: [
                'group.id' => 'products-service',
                'auto.offset.reset' => 'earliest',
                'max.poll.interval.ms' => 300_000,
                'session.timeout.ms' => 45_000,
                'heartbeat.interval.ms' => 3_000,
            ],
            autoCommit: true,
            consumerTimeout: 5_000,
        ),
    ));
```

Параметры `Worker`:

| Параметр | Тип | По умолчанию | Описание |
|---|---|---|---|
| `name` | `string` | — | Уникальное имя воркера в реестре |
| `topics` | `list<Topic>` | — | Топики, на которые подписывается воркер |
| `options` | `Options` | `new Options()` | Параметры консьюмера |

Параметры `Options`:

| Параметр | Тип | По умолчанию | Описание |
|---|---|---|---|
| `additionalOptions` | `array<string, scalar\|null>` | `[]` | Параметры консьюмера librdkafka |
| `autoCommit` | `bool` | `true` | Режим коммита оффсетов, см. [Коммит оффсетов](#коммит-оффсетов) |
| `consumerTimeout` | `int` | `2000` | Сколько `consume()` ждёт сообщение, мс |

::: warning Фильтрация additionalOptions
Ключи фильтруются по белому списку `Options::CONSUMER_OPTIONS` подключения — параметры продюсера вроде `compression.codec` молча отбрасываются. Полный список — в разделе [Конфигурация](/ru/docs/2.x/configuration#фильтрация-параметров-продюсера-и-консьюмера).
:::

Каждому топику, который читает воркер, нужен маршрут в шине. Сообщение из топика без маршрута приводит к ошибке `dispatch()` и останавливает воркер — см. [Маршруты консьюмера](/ru/docs/2.x/consumer/configure).

## Запуск воркера

`WorkerRunnerFactory` принимает шину и имя воркера и возвращает `WorkerRunner`. Раннер создаёт консьюмер на `$bus->connection()` и передаёт каждое сообщение в `$bus->dispatch()`:

```php
use KafkaBus\Worker\WorkerRunnerFactory;

$runner = (new WorkerRunnerFactory($workerRegistry))
    ->create($bus, 'products');

$runner->run(); // блокирующий цикл
```

`run()` не возвращает управление, пока не вызван `forceStop()` или ошибка не остановила цикл. На каждой итерации:

1. `Consumer::getMessage()` ждёт сообщение до `consumerTimeout` мс;
2. `$bus->dispatch()` выполняет маршрутизацию, middleware и обработчик;
3. `Consumer::commit()` подтверждает оффсет — явно при `autoCommit: false`, см. [Коммит оффсетов](#коммит-оффсетов).

Воркер коммитит только после успешного `dispatch()`. Если он бросил `MessageConsumerNotHandledException`, исключение выходит из `run()`, и воркер это сообщение не коммитит.

::: tip Как не ронять воркер на ошибках
Чтобы одно «плохое» сообщение не останавливало обработку, перехватите исключение в middleware маршрута — см. [Middleware с обработкой ошибок](/ru/docs/2.x/consumer/pipeline#middleware-с-обработкои-ошибок).
:::

Параметры конструктора `WorkerRunnerFactory`:

| Параметр | Тип | По умолчанию | Описание |
|---|---|---|---|
| `workerRegistry` | `WorkerRegistryInterface` | `new MemoryWorkerRegistry()` | Где искать воркеры по имени |
| `streamFactory` | `ConsumerStreamFactoryInterface` | `new ConsumerStreamFactory()` | Собирает цикл чтения |
| `workerMerger` | `WorkerMerger` | `new WorkerMerger()` | Объединяет несколько воркеров в один |

Неизвестное имя воркера приводит к `WorkerException`.

## Несколько воркеров в одном процессе

Передайте список имён, чтобы читать топики нескольких воркеров одним консьюмером:

```php
$runner = (new WorkerRunnerFactory($workerRegistry))
    ->create($bus, ['products', 'orders']);

$runner->run();
```

`WorkerMerger` объединяет воркеры в синтетический `grouped-worker`: списки топиков сливаются (дубли по имени топика убираются), параметры берутся у первого воркера. Список из одного имени работает как обычная строка.

::: warning
Используются только `additionalOptions`, `autoCommit` и `consumerTimeout` первого воркера. Объединяйте только воркеры с одинаковыми `group.id` и режимом коммита; остальные запускайте отдельными процессами.
:::

Поскольку обработчики живут в шине, объединение воркеров не влияет на то, какой обработчик и middleware обработают сообщение, — маршрутизация всегда идёт по топику.

## Коммит оффсетов

Параметр `autoCommit` определяет, кто коммитит оффсеты группы консьюмеров — librdkafka в фоне или воркер после каждого обработанного сообщения:

| `autoCommit` | `enable.auto.commit` | Коммитер | Поведение |
|---|---|---|---|
| `true` (по умолчанию) | `'true'` | `VoidCommiter` | librdkafka коммитит оффсеты в фоне каждые `auto.commit.interval.ms`; сам воркер не коммитит |
| `false` | `'false'` | `DefaultCommiter` | После каждого успешного `dispatch()` воркер вызывает `KafkaConsumer::commitAsync()` для этого сообщения |

Что выбрать:

- **`autoCommit: false`** — оффсет сдвигается только после успешной работы обработчика. Если процесс упадёт, необработанное сообщение будет прочитано снова (at-least-once). Рекомендуется, когда потеря сообщения недопустима.
- **`autoCommit: true`** — меньше запросов к брокеру, но фоновый коммит не зависит от результата обработки: после падения прочитанное, но не обработанное сообщение может быть пропущено.

::: warning
`autoCommit` всегда перезаписывает `enable.auto.commit` — задавать его в `additionalOptions` бесполезно.
:::

::: info Исправлено в 2.1.1
До `kafka-bus/core` 2.1.1 коммитеры были перепутаны: при `autoCommit: false` не коммитилось ничего. Обновитесь до `^2.1.1`, если полагаетесь на явный коммит.
:::

Явный коммит выполняется через `RetryRepeater`: до 10 попыток с паузой 1000 мкс.

## Корректная остановка

```php
pcntl_async_signals(true);

$runner = (new WorkerRunnerFactory($workerRegistry))->create($bus, 'products');

pcntl_signal(SIGINT, fn () => $runner->forceStop());  // Ctrl+C
pcntl_signal(SIGTERM, fn () => $runner->forceStop()); // kill

$runner->run();
```

`forceStop()` выставляет флаг, который проверяется в конце каждой итерации цикла. Процесс останавливается только после того, как текущее сообщение передано в шину и для него выполнен коммит. После выхода из цикла консьюмер отписывается от топиков.

::: tip
В Laravel это уже сделано: команда `kafka:consume` обрабатывает `SIGTERM`, `SIGINT` и `SIGQUIT`.
:::

## Устойчивость цикла

Цикл чтения игнорирует штатные «пустые» ответы брокера и продолжает работу:

| Код ошибки | Причина |
|---|---|
| `RD_KAFKA_RESP_ERR__PARTITION_EOF` | Достигнут конец партиции |
| `RD_KAFKA_RESP_ERR__TRANSPORT` | Временная сетевая ошибка |
| `RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT` | Брокер не ответил вовремя |
| `RD_KAFKA_RESP_ERR__TIMED_OUT` | Истёк `consumerTimeout` |

Любой другой код пробрасывается как `MessageConsumerException` и останавливает воркер.

::: tip
`RD_KAFKA_RESP_ERR__TIMED_OUT` возникает каждые `consumerTimeout` мс, когда новых сообщений нет. Это не ошибка — цикл просто переходит к следующей итерации.
:::

## Рекомендуемые параметры консьюмера

| Параметр librdkafka | Описание | Рекомендуемое значение |
|---|---|---|
| `group.id` | Имя группы консьюмеров | Имя вашего сервиса |
| `auto.offset.reset` | Стартовая позиция, если у группы нет оффсета | `earliest` или `latest` |
| `max.poll.interval.ms` | Максимальное время между опросами | `300000` |
| `session.timeout.ms` | Таймаут сессии | `45000` |
| `heartbeat.interval.ms` | Интервал heartbeat | `3000` |

## Собственный реестр воркеров

`WorkerRunnerFactory` принимает любой `WorkerRegistryInterface`. Реализуйте его, чтобы загружать воркеры из конфигурации или базы данных, — у интерфейса один метод, который возвращает `Worker` или `null`:

```php
use KafkaBus\Worker\Registry\WorkerRegistryInterface;
use KafkaBus\Worker\Worker;

final class ConfigWorkerRegistry implements WorkerRegistryInterface
{
    public function get(string $workerName): ?Worker
    {
        // ...
    }
}
```

Laravel-бридж использует это, чтобы собирать воркеры из `config/kafka-bus.php`.
