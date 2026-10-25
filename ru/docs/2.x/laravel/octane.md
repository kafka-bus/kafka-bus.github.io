---
title: Laravel Octane
description: Как Laravel-бридж Kafka Bus прогревает шину в воркерах Octane
---

# Laravel Octane

Laravel Bridge включает автоматическую поддержку [Laravel Octane](https://laravel.com/docs/octane). Никакой дополнительной настройки не требуется.

## Как это работает

При установленном `laravel/octane` сервис-провайдер автоматически регистрирует слушатель события `WorkerStarting`. Этот слушатель резолвит `BusInterface` из контейнера в момент запуска воркера, устанавливая соединение с Kafka до обработки первого запроса.

Без этого прогрева соединение инициализировалось бы лениво при первом входящем запросе — что может добавить задержку или вызвать проблемы в модели долгоживущих воркеров Octane.

## Настройка

Установите Laravel Octane обычным способом:

```bash
composer require laravel/octane
php artisan octane:install
```

Никаких изменений в `config/kafka-bus.php` не требуется. Интеграция с Octane активируется автоматически при наличии пакета `laravel/octane`.
