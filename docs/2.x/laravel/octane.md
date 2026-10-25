---
title: Laravel Octane
description: How the Kafka Bus Laravel bridge warms up the bus in Octane workers
---

# Laravel Octane

The Laravel bridge includes automatic support for [Laravel Octane](https://laravel.com/docs/octane). No additional configuration is required.

## How it works

When `laravel/octane` is installed, the service provider automatically registers a listener for the `WorkerStarting` event. The listener resolves `BusInterface` from the container at worker boot time, establishing the Kafka connection before the first request is handled.

Without this warm-up, the connection would be initialized lazily on the first incoming request — potentially adding latency or causing issues under Octane's persistent worker model.

## Setup

Install Laravel Octane as usual:

```bash
composer require laravel/octane
php artisan octane:install
```

No changes to `config/kafka-bus.php` are needed. The Octane integration is activated automatically when the `laravel/octane` package is present.
