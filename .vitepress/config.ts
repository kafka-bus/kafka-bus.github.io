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
      { text: 'Docs', link: '/docs/installation' },
      {
        text: 'Components',
        items: [
          { text: 'Messages', link: '/docs/components/messages' },
          { text: 'Commiter', link: '/docs/components/commiter' },
        ],
      },
      {
        text: 'Integrations',
        items: [
          { text: 'Laravel', link: '/docs/laravel/installation' },
        ],
      },
      {
        text: 'v1.x',
        items: [
          { text: 'Changelog', link: 'https://github.com/kafka-bus/kafka-bus/blob/1.x/CHANGELOG.md' },
        ],
      },
    ],

    sidebar: {
      '/docs/': [
        { text: 'Установка', link: '/docs/installation' },
        { text: 'Конфигурация', link: '/docs/configuration' },
        { text: 'Архитектура', link: '/docs/architecture' },
        { text: 'Топики', link: '/docs/topics' },

        {
          text: 'Producer',
          items: [
            { text: 'Producing messages', link: '/docs/producer/producing' },
            { text: 'Configure', link: '/docs/producer/configure' },
            { text: 'Pipeline', link: '/docs/producer/pipeline' },
          ],
        },

        {
          text: 'Consumer',
          items: [
            { text: 'Consuming messages', link: '/docs/consumer/consuming' },
            { text: 'Configure', link: '/docs/consumer/configure' },
            { text: 'Pipeline', link: '/docs/consumer/pipeline' },
          ],
        },

        {
          text: 'Components',
          items: [
            { text: 'Messages', link: '/docs/components/messages' },
            { text: 'Commiter', link: '/docs/components/commiter' },
          ],
        },

        {
          text: 'Laravel',
          items: [
            { text: 'Установка', link: '/docs/laravel/installation' },
            { text: 'Конфигурация', link: '/docs/laravel/configuration' },
            { text: 'Producer', link: '/docs/laravel/producers' },
            { text: 'Consumer', link: '/docs/laravel/consumers' },
            { text: 'Artisan-команды', link: '/docs/laravel/commands' },
            { text: 'Тестирование', link: '/docs/laravel/testing' },
          ],
        },
      ],
    },

    socialLinks: [
      { icon: 'github', link: 'https://github.com/kafka-bus/kafka-bus' },
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
