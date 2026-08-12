# Установка

## Требования

| Зависимость   | Версия                                    |
|---------------|-------------------------------------------|
| PHP           | `^8.2`                                    |
| Laravel       | `^10.0 \|\| ^11.0 \|\| ^12.0 \|\| ^13.0` |
| `ext-rdkafka` | любая актуальная                          |

Пакет тянет за собой `kafka-bus/core`, `kafka-bus/commiter` и `kafka-bus/messages` — устанавливать их отдельно не нужно.

## Установка

```bash
composer require kafka-bus/laravel-bridge
```

Пакет использует автодискавери. Регистрируются **два** провайдера:

| Провайдер                   | Что регистрирует                                             |
|-----------------------------|--------------------------------------------------------------|
| `KafkaBusServiceProvider`   | `BusInterface`, реестры подключений и топиков, Artisan-команды |
| `CommiterServiceProvider`   | `RepositorySourceInterface`, `ConsumerMessageRepositoryInterface` |

## Публикация конфига

```bash
php artisan vendor:publish --tag=kafka-bus
```

Создаётся `config/kafka-bus.php` с полной конфигурацией подключений, топиков, продьюсеров и консьюмеров.

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

### SASL аутентификация

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
        # Можно задать на уровне воркера, если коммит нужен не для всех топиков
        KafkaBus\Commiter\Middleware\ConsumerCommiterMiddleware::class,
    ],
],

'producers' => [
    'middleware' => [
        KafkaBus\Commiter\Middleware\ProducerIdempotencyMiddleware::class,
    ],
],
```

Middleware резолвятся через контейнер Laravel, поэтому `ConsumerCommiterMiddleware` автоматически получит настроенный репозиторий из `CommiterServiceProvider`.

Подробнее — в разделах [Commiter](/ru/docs/components/commiter) и [Конфигурация](/ru/docs/laravel/configuration#commiter).

## Поддержка Octane

Если установлен `laravel/octane`, провайдер подписывается на событие `WorkerStarting` и «прогревает» `BusInterface` при старте воркера. Дополнительная настройка не требуется.

Пакет `laravel/octane` указан в `suggest` — ставить его только ради kafka-bus не нужно.

## Проверка установки

```bash
# Список зарегистрированных воркеров
php artisan kafka:worker:list

# Список маршрутов продьюсера
php artisan kafka:route:list
```

Обе команды читают конфиг и резолвят обработчики через контейнер, поэтому опечатка в имени класса обнаружится сразу.

## Что дальше

- [Конфигурация](/ru/docs/laravel/configuration) — полный разбор `config/kafka-bus.php`
- [Producer](/ru/docs/laravel/producers) — публикация сообщений через Facade и DI
- [Consumer](/ru/docs/laravel/consumers) — создание воркеров и обработчиков
- [Artisan команды](/ru/docs/laravel/commands) — управление воркерами и offset
