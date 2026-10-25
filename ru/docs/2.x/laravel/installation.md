---
title: Установка в Laravel
description: Установка Laravel-бриджа Kafka Bus, публикация конфига и настройка окружения
---

# Установка

`kafka-bus/laravel-bridge` встраивает Kafka Bus в Laravel-приложение: файл конфигурации, привязки в контейнере, фасад `KafkaBus`, Artisan-команды и фейк для тестов.

## Требования

| Зависимость   | Версия                                    |
|---------------|-------------------------------------------|
| PHP           | `^8.2`                                    |
| Laravel       | `^10.0 \|\| ^11.0 \|\| ^12.0 \|\| ^13.0` |
| `ext-rdkafka` | любая актуальная                          |

Пакет зависит от мета-пакета `kafka-bus/kafka-bus:^2.1`, в который входят `core`, `worker`, `metadata`, `messages` и `commiter`, — устанавливать их отдельно не нужно.

## Установка

```bash
composer require kafka-bus/laravel-bridge
```

Пакет использует автодискавери. Регистрируются **два** провайдера:

| Провайдер                   | Что регистрирует                                             |
|-----------------------------|--------------------------------------------------------------|
| `KafkaBusServiceProvider`   | `BusInterface`, подключение по умолчанию, реестр топиков, ресивер, реестр воркеров, Artisan-команды |
| `CommiterServiceProvider`   | `RepositorySourceInterface`, `ConsumerMessageRepositoryInterface` |

## Публикация конфигурации

```bash
php artisan vendor:publish --tag=kafka-bus
```

Создаётся `config/kafka-bus.php` с полной конфигурацией подключений, топиков, продюсеров и консьюмеров.

::: tip
Публиковать конфиг необязательно: провайдер подмешивает значения по умолчанию через `mergeConfigFrom()`. Но без публикации нельзя задать топики и маршруты, так что на практике это первый шаг.
:::

## Настройка .env

Минимальный набор переменных для подключения к брокеру:

```dotenv
# Адрес брокера
KAFKA_BROKER_LIST=localhost:9092

# Префикс топиков (по умолчанию = APP_ENV + '.')
KAFKA_PREFIX=production.

# Группа консьюмеров
KAFKA_CONSUMER_GROUP_ID=my-service

# Подключение (по умолчанию 'kafka')
KAFKA_CONNECTION=kafka
```

::: warning Брокер без аутентификации
Конфиг по умолчанию задаёт `KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT`. Для локального брокера без SASL переопределите протокол, иначе подключение не установится:

```dotenv
KAFKA_SECURITY_PROTOCOL=PLAINTEXT
```
:::

### SASL-аутентификация

```dotenv
KAFKA_BROKER_LIST=pkc-xxx.us-east-1.aws.confluent.cloud:9092
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISMS=PLAIN
KAFKA_SASL_USERNAME=your-api-key
KAFKA_SASL_PASSWORD=your-api-secret
```

## Установка Commiter

Если нужна идемпотентная обработка сообщений, опубликуйте конфиг и миграции Commiter:

```bash
php artisan vendor:publish --tag=kafka-bus-commiter
php artisan migrate
```

Создаётся:
- `config/kafka-bus-commiter.php`
- миграция таблицы `kafka_bus_commits` (колонки `key`, `number`, `commited_at`)

Включите middleware в конфиге:

```php
// config/kafka-bus.php
'consumers' => [
    'middleware' => [
        // Или укажите в конкретных маршрутах, если он нужен не всем топикам
        KafkaBus\Commiter\Middleware\ConsumerCommiterMiddleware::class,
    ],
],

'producers' => [
    'middleware' => [
        KafkaBus\Commiter\Middleware\PublisherIdempotencyMiddleware::class,
    ],
],
```

Middleware резолвятся через контейнер Laravel, поэтому `ConsumerCommiterMiddleware` автоматически получит настроенный репозиторий из `CommiterServiceProvider`.

Подробнее — в разделах [Commiter](/ru/docs/2.x/components/commiter) и [Конфигурация](/ru/docs/2.x/laravel/configuration#commiter).

## Поддержка Octane

Если установлен `laravel/octane`, провайдер подписывается на событие `WorkerStarting` и «прогревает» `BusInterface` при старте воркера. Дополнительная настройка не требуется.

Пакет `laravel/octane` указан в `suggest` — ставить его только ради kafka-bus не нужно.

## Проверка установки

```bash
# Маршруты консьюмера: топик → обработчик и middleware
php artisan kafka:consumer:route:list

# Воркеры и их топики
php artisan kafka:worker:list

# Маршруты продюсера: класс сообщения → топик
php artisan kafka:publisher:route:list
```

Команды маршрутов резолвят обработчики и middleware через контейнер, поэтому опечатка в имени класса обнаружится сразу.

## Что дальше

- [Конфигурация](/ru/docs/2.x/laravel/configuration) — полный разбор `config/kafka-bus.php`
- [Отправка сообщений](/ru/docs/2.x/laravel/producers) — публикация через фасад и внедрение зависимостей
- [Получение сообщений](/ru/docs/2.x/laravel/consumers) — обработчики, маршруты и воркеры
- [Artisan-команды](/ru/docs/2.x/laravel/commands) — запуск воркеров и управление оффсетами
