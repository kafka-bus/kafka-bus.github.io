# Установка

## Требования

| Зависимость          | Версия           | Обязательно   |
|----------------------|------------------|---------------|
| PHP                  | `^8.2`           | ✅             |
| `ext-rdkafka`        | любая актуальная | ✅             |
| Кластер Apache Kafka | `2.x+`           | ✅             |
| `ext-pcntl`          | любая            | Рекомендуется |

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

## Дополнительные пакеты

В зависимости от вашего стека можно установить интеграционные пакеты:

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

- [Конфигурация соединений](/docs/configuration)
- [Регистрация топиков](/docs/topics)
- [Пример использования](https://github.com/kafka-bus/kafka-bus/tree/1.x/examples)
