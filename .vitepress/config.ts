import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'Kafka Bus',
  description: 'PHP-клиент для Apache Kafka на базе ext-rdkafka',

  head: [
    ['link', { rel: 'icon', href: '/favicon.ico' }],
  ],

  themeConfig: {
    logo: '/logo.svg',

    nav: [
      { text: 'Руководство', link: '/guide/quickstart' },
      { text: 'Ядро', link: '/core/installation' },
      { text: 'Laravel', link: '/laravel/installation' },
      {
        text: 'Пакеты',
        items: [
          { text: 'kafka-bus-messages', link: '/packages/messages' },
          { text: 'kafka-bus-commiter', link: '/packages/commiter' },
          { text: 'kafka-bus-outbox', link: '/packages/outbox' },
        ],
      },
      {
        text: 'v1.1.1',
        items: [
          { text: 'Changelog', link: 'https://github.com/micromus/kafka-bus/blob/1.x/CHANGELOG.md' },
          { text: 'Packagist', link: 'https://packagist.org/packages/micromus/kafka-bus' },
        ],
      },
    ],

    sidebar: {
      '/guide/': [
        {
          text: 'Начало работы',
          items: [
            { text: 'Быстрый старт', link: '/guide/quickstart' },
          ],
        },
      ],

      '/core/': [
        {
          text: 'Ядро (kafka-bus)',
          items: [
            { text: 'Установка', link: '/core/installation' },
            { text: 'Конфигурация', link: '/core/configuration' },
            { text: 'Топики', link: '/core/topics' },
            { text: 'Producer', link: '/core/producers' },
            { text: 'Consumer', link: '/core/consumers' },
            { text: 'Pipeline (Middleware)', link: '/core/pipeline' },
            { text: 'Тестирование', link: '/core/testing' },
            { text: 'Архитектура', link: '/core/architecture' },
          ],
        },
      ],

      '/laravel/': [
        {
          text: 'Laravel',
          items: [
            { text: 'Установка', link: '/laravel/installation' },
            { text: 'Конфигурация', link: '/laravel/configuration' },
            { text: 'Producer', link: '/laravel/producers' },
            { text: 'Consumer', link: '/laravel/consumers' },
            { text: 'Artisan-команды', link: '/laravel/commands' },
            { text: 'Тестирование', link: '/laravel/testing' },
          ],
        },
      ],

      '/packages/': [
        {
          text: 'Дополнительные пакеты',
          items: [
            { text: 'Messages', link: '/packages/messages' },
            { text: 'Commiter', link: '/packages/commiter' },
            { text: 'Outbox', link: '/packages/outbox' },
          ],
        },
      ],
    },

    socialLinks: [
      { icon: 'github', link: 'https://github.com/micromus/kafka-bus' },
    ],

    footer: {
      message: 'Released under the MIT License.',
      copyright: 'Copyright © 2024-present Kirill Popkov',
    },

    search: {
      provider: 'local',
    },

    editLink: {
      pattern: 'https://github.com/kafka-bus/kafka-bus.github.io/edit/main/:path',
      text: 'Редактировать эту страницу',
    },

    lastUpdated: {
      text: 'Обновлено',
    },
  },
})
