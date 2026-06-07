# Установка

## Требования

| Зависимость | Версия | Обязательно |
|---|---|---|
| PHP | `^8.2` | ✅ |
| `ext-rdkafka` | любая актуальная | ✅ |
| Кластер Apache Kafka | `2.x+` | ✅ |
| `ext-pcntl` | любая | Рекомендуется |

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
composer require micromus/kafka-bus
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
composer require micromus/kafka-bus-laravel
```

```bash [Spiral]
composer require micromus/kafka-bus-spiral
```

:::

Для типизированных сообщений:

```bash
composer require micromus/kafka-bus-messages
```

Для идемпотентной обработки:

```bash
composer require micromus/kafka-bus-commiter
```

Для паттерна Outbox:

```bash
composer require micromus/kafka-bus-outbox
```

## Что дальше

- [Конфигурация соединений](/core/configuration)
- [Регистрация топиков](/core/topics)
- [Быстрый старт](/guide/quickstart)
