---
layout: home

hero:
  name: Kafka Bus
  text: PHP-клиент для Apache Kafka
  tagline: Типизированные сообщения, middleware-конвейер и удобное тестирование без лишнего кода
  actions:
    - theme: brand
      text: Быстрый старт
      link: /guide/quickstart
    - theme: alt
      text: Установка ядра
      link: /core/installation
    - theme: alt
      text: GitHub
      link: https://github.com/micromus/kafka-bus

features:
  - icon: 🚌
    title: kafka-bus
    details: Ядро экосистемы. Bus-фасад с поддержкой нескольких соединений, маршрутизацией топиков, конвейером middleware и встроенными фейками для тестирования.
    link: /core/installation
    linkText: Документация ядра

  - icon: 🟥
    title: kafka-bus-laravel
    details: Laravel-интеграция с автодискавери, конфигом из коробки, Artisan-командами для управления воркерами и KafkaBus::fake() для тестов.
    link: /laravel/installation
    linkText: Документация Laravel

  - icon: 📨
    title: kafka-bus-messages
    details: Типизированные Payload, JsonMessage и DomainMessage с автоматическим кастингом полей, поддержкой событий create/update/delete и тест-фабриками.
    link: /packages/messages
    linkText: Документация Messages

  - icon: ✅
    title: kafka-bus-commiter
    details: Идемпотентная обработка сообщений через middleware. Отслеживает обработанные сообщения, пропускает дубликаты и ограничивает количество попыток.
    link: /packages/commiter
    linkText: Документация Commiter

  - icon: 📤
    title: kafka-bus-outbox
    details: Реализация паттерна Transactional Outbox для надёжной публикации сообщений в Kafka в связке с транзакциями базы данных.
    link: /packages/outbox
    linkText: Документация Outbox

  - icon: 🌀
    title: kafka-bus-spiral
    details: Интеграция с фреймворком Spiral. Регистрирует Bus и воркеры через bootloaders, поддерживает конфигурацию через ENV и Spiral Container.
    link: https://github.com/micromus/kafka-bus-spiral
    linkText: GitHub
---
