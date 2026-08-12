# Установка

## Требования

| Зависимость          | Версия           | Обязательно   |
|----------------------|------------------|---------------|
| PHP                  | `^8.2`           | ✅            |
| `ext-rdkafka`        | любая актуальная | ✅            |
| Apache Kafka cluster | `2.x+`           | ✅            |
| `ext-pcntl`          | любая            | Рекомендуется |

`ext-rdkafka` — обязательное требование `kafka-bus/core`: весь низкоуровневый ввод-вывод идёт через `RdKafka\Producer` и `RdKafka\KafkaConsumer`.

`ext-pcntl` формально не требуется, но без него нельзя корректно остановить долгоживущий процесс консьюмера по `SIGTERM`/`SIGINT` — см. [Плавная остановка](/ru/docs/consumer/consuming#плавная-остановка-по-сигналам).

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

```bash
composer require kafka-bus/core
```

## Проверка

```bash
php -m | grep rdkafka
# rdkafka

php -r "echo \RdKafka\Conf::class;"
# RdKafka\Conf
```

## Состав экосистемы

| Пакет                      | Namespace           | Назначение                                                         |
|----------------------------|---------------------|--------------------------------------------------------------------|
| `kafka-bus/core`           | `KafkaBus\Core`     | Bus, подключения, маршрутизация, pipeline продюсера и консьюмера  |
| `kafka-bus/messages`       | `KafkaBus\Messages` | Типизированный `Payload`, кастеры, `DomainMessage`, фабрики тестов |
| `kafka-bus/commiter`       | `KafkaBus\Commiter` | Идемпотентная обработка: middleware консьюмера и продюсера        |
| `kafka-bus/laravel-bridge` | `KafkaBus\Laravel`  | Интеграция с Laravel: конфиг, фасад, Artisan-команды, `FakeBus`    |

## Дополнительные пакеты

В зависимости от вашего стека установите пакеты интеграции:

::: code-group

```bash [Laravel]
composer require kafka-bus/laravel-bridge
```

```bash [Spiral]
composer require kafka-bus/spiral-bridge # В разработке
```

:::

Для типизированных сообщений:

```bash
composer require kafka-bus/messages
```

Для идемпотентной обработки:

```bash
composer require kafka-bus/commiter
```

Для паттерна Outbox (в разработке):

```bash
composer require kafka-bus/outbox
```

## Что дальше

- [Конфигурация подключений](/ru/docs/configuration)
- [Регистрация топиков](/ru/docs/topics)
- [Примеры использования](https://github.com/kafka-bus/kafka-bus/tree/1.x/examples)
