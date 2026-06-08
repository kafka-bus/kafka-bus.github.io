---
layout: home

hero:
  name: Kafka Bus
  text: PHP client for Apache Kafka
  tagline: Typed messages, middleware pipeline, and convenient testing without boilerplate
  actions:
    - theme: brand
      text: Quick Start
      link: /docs/core/installation
    - theme: alt
      text: GitHub
      link: https://github.com/kafka-bus/kafka-bus

features:
  - icon: 🚌
    title: kafka-bus/core
    details: The ecosystem core. A Bus facade with multi-connection support, topic routing, a middleware pipeline, and built-in fakes for testing.
    link: /docs/core/installation
    linkText: Core Documentation

  - icon: 🟥
    title: kafka-bus/laravel-bridge
    details: Laravel integration with auto-discovery, out-of-the-box config, Artisan commands for managing workers, and KafkaBus::fake() for tests.
    link: /laravel/installation
    linkText: Laravel Documentation

  - icon: 📨
    title: kafka-bus/messages
    details: Typed Payload, JsonMessage, and DomainMessage with automatic field casting, create/update/delete event support, and test factories.
    link: /packages/messages
    linkText: Messages Documentation

  - icon: ✅
    title: kafka-bus/commiter
    details: Idempotent message processing via middleware. Tracks processed messages, skips duplicates, and limits the number of retry attempts.
    link: /packages/commiter
    linkText: Commiter Documentation
---
