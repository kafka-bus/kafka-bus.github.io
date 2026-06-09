# Установка

## Требования

| Зависимость   | Версия                         |
|---------------|--------------------------------|
| PHP           | `^8.2`                         |
| Laravel       | `^10.0 \|\| ^11.0 \|\| ^12.0` |
| `ext-rdkafka` | любая актуальная               |

## Установка

```bash
composer require kafka-bus/laravel-bridge
```

Пакет использует автодискавери — `KafkaBusServiceProvider` регистрируется автоматически.

## Публикация конфига

```bash
php artisan vendor:publish --tag=kafka-bus
```

Создаётся `config/kafka-bus.php` с полной конфигурацией подключений, топиков, продьюсеров и консьюмеров.

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
- Миграция для таблицы `kafka_bus_commits`

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

Подробнее — в разделе [Commiter](/ru/docs/components/commiter).

## Проверка установки

```bash
# Список зарегистрированных воркеров
php artisan kafka:worker:list

# Список маршрутов продьюсера
php artisan kafka:route:list
```

## Что дальше

- [Конфигурация](/ru/docs/laravel/configuration) — полный разбор `config/kafka-bus.php`
- [Producer](/ru/docs/laravel/producers) — публикация сообщений через Facade и DI
- [Consumer](/ru/docs/laravel/consumers) — создание воркеров и обработчиков
- [Artisan команды](/ru/docs/laravel/commands) — управление воркерами и offset
