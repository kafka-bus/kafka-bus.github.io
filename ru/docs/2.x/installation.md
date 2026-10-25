---
title: Установка
description: Требования и установка Kafka Bus 2.x — мета-пакет и отдельные компоненты
---

# Установка

Kafka Bus — PHP-клиент для Apache Kafka поверх `ext-rdkafka`. На этой странице — требования, установка одной командой и выбор отдельных пакетов, если весь набор не нужен.

Документация описывает Kafka Bus **2.x**. Если вы обновляетесь с 1.x, см. [Переход с 1.x](/ru/docs/2.x/upgrade).

## Требования

| Зависимость | Версия | Обязательно |
|---|---|---|
| PHP | `^8.2` | ✅ |
| `ext-rdkafka` | любая актуальная | ✅ |
| Кластер Apache Kafka | `2.x+` | ✅ |
| `ext-pcntl` | любая | Рекомендуется |

`ext-rdkafka` нужен всем пакетам: весь низкоуровневый ввод-вывод идёт через `RdKafka\Producer` и `RdKafka\KafkaConsumer`.

`ext-pcntl` не обязателен, но без него долгоживущий воркер нельзя корректно остановить по `SIGTERM`/`SIGINT` — см. [Корректная остановка](/ru/docs/2.x/consumer/workers#корректная-остановка).

::: details Как установить ext-rdkafka
```bash
# Ubuntu / Debian
sudo apt-get install librdkafka-dev
pecl install rdkafka
echo "extension=rdkafka.so" >> /etc/php/8.2/cli/php.ini

# macOS (Homebrew)
brew install librdkafka
pecl install rdkafka

# Docker (официальный образ php)
RUN apt-get install -y librdkafka-dev \
    && pecl install rdkafka \
    && docker-php-ext-enable rdkafka
```
:::

## Установка пакета

Установите мета-пакет — в нём все компоненты инструмента:

```bash
composer require kafka-bus/kafka-bus
```

Используете Laravel? Установите бридж — он уже подтягивает `kafka-bus/kafka-bus`:

```bash
composer require kafka-bus/laravel-bridge
```

См. [Установка в Laravel](/ru/docs/2.x/laravel/installation).

## Проверка

```bash
php -m | grep rdkafka
# rdkafka

php -r "echo \RdKafka\Conf::class;"
# RdKafka\Conf
```

## Установка отдельных компонентов

Мета-пакет `kafka-bus/kafka-bus` заменяет пять пакетов. Можно поставить только нужные — все они выпускаются вместе под одним тегом версии.

| Пакет | Пространство имён | Назначение | Зависит от |
|---|---|---|---|
| `kafka-bus/core` | `KafkaBus\Core` | `Bus`, подключения, топики, маршрутизация, pipeline публикации и приёма | — |
| `kafka-bus/worker` | `KafkaBus\Worker` | Цикл чтения Kafka, передающий сообщения в `Bus` | `core` |
| `kafka-bus/metadata` | `KafkaBus\Metadata` | Список топиков и партиций, оффсеты групп консьюмеров | `core` |
| `kafka-bus/messages` | `KafkaBus\Messages` | Типизированный `Payload`, кастеры, `DomainMessage`, тестовые фабрики | `core` |
| `kafka-bus/commiter` | `KafkaBus\Commiter` | Middleware идемпотентной обработки | `core`, `psr/log` |
| `kafka-bus/laravel-bridge` | `KafkaBus\Laravel` | Интеграция с Laravel: конфиг, фасад, Artisan-команды, `FakeBus` | `kafka-bus/kafka-bus` |

Типовые наборы:

| Сценарий | Пакеты |
|---|---|
| Сервис только публикует сообщения | `kafka-bus/core` |
| Сервис читает топики | `kafka-bus/core`, `kafka-bus/worker` |
| Просмотр и сброс оффсетов | добавьте `kafka-bus/metadata` |
| Типизированные сообщения | добавьте `kafka-bus/messages` |
| Защита от дублей | добавьте `kafka-bus/commiter` |

```bash
composer require kafka-bus/core kafka-bus/worker
```

## Что дальше

- [Настройка подключения](/ru/docs/2.x/configuration)
- [Архитектура](/ru/docs/2.x/architecture)
- [Регистрация топиков](/ru/docs/2.x/topics)
- [Примеры использования](https://github.com/kafka-bus/kafka-bus/tree/2.x/examples)
