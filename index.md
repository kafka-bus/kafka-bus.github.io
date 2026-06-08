---
layout: home

hero:
  name: Kafka Bus
  text: PHP-клиент для Apache Kafka
  tagline: Типизированные сообщения, middleware-конвейер и удобное тестирование без лишнего кода
  actions:
    - theme: brand
      text: Быстрый старт
      link: /docs/core/installation
    - theme: alt
      text: GitHub
      link: https://github.com/kafka-bus/kafka-bus

features:
  - icon: 🚌
    title: kafka-bus/core
    details: Ядро экосистемы. Bus-фасад с поддержкой нескольких соединений, маршрутизацией топиков, конвейером middleware и встроенными фейками для тестирования.
    link: /docs/core/installation
    linkText: Документация ядра

  - icon: 🟥
    title: kafka-bus/laravel-bridge
    details: Laravel-интеграция с автодискавери, конфигом из коробки, Artisan-командами для управления воркерами и KafkaBus::fake() для тестов.
    link: /laravel/installation
    linkText: Документация Laravel

  - icon: 📨
    title: kafka-bus/messages
    details: Типизированные Payload, JsonMessage и DomainMessage с автоматическим кастингом полей, поддержкой событий create/update/delete и тест-фабриками.
    link: /packages/messages
    linkText: Документация Messages

  - icon: ✅
    title: kafka-bus/commiter
    details: Идемпотентная обработка сообщений через middleware. Отслеживает обработанные сообщения, пропускает дубликаты и ограничивает количество попыток.
    link: /packages/commiter
    linkText: Документация Commiter
---
