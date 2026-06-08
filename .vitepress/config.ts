import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'Kafka Bus',

  head: [
    ['link', { rel: 'icon', href: '/favicon.ico' }],
  ],

  locales: {
    root: {
      label: 'English',
      lang: 'en-US',
      description: 'PHP client for Apache Kafka',
      themeConfig: {
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
            { text: 'Installation', link: '/docs/installation' },
            { text: 'Configuration', link: '/docs/configuration' },
            { text: 'Architecture', link: '/docs/architecture' },
            { text: 'Topics', link: '/docs/topics' },
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
                { text: 'Installation', link: '/docs/laravel/installation' },
                { text: 'Configuration', link: '/docs/laravel/configuration' },
                { text: 'Producing', link: '/docs/laravel/producers' },
                { text: 'Consuming', link: '/docs/laravel/consumers' },
                { text: 'Commands', link: '/docs/laravel/commands' },
                { text: 'Testing', link: '/docs/laravel/testing' },
              ],
            },
          ],
        },

        editLink: {
          pattern: 'https://github.com/kafka-bus/kafka-bus.github.io/edit/main/:path',
          text: 'Edit this page',
        },
        lastUpdated: { text: 'Updated' },
        footer: {
          message: 'Released under the MIT License.',
          copyright: 'Copyright © 2024-present Kirill Popkov',
        },
      },
    },

    ru: {
      label: 'Русский',
      lang: 'ru-RU',
      description: 'PHP клиент для Apache Kafka',
      themeConfig: {
        nav: [
          { text: 'Документация', link: '/ru/docs/installation' },
          {
            text: 'Компоненты',
            items: [
              { text: 'Messages', link: '/ru/docs/components/messages' },
              { text: 'Commiter', link: '/ru/docs/components/commiter' },
            ],
          },
          {
            text: 'Интеграции',
            items: [
              { text: 'Laravel', link: '/ru/docs/laravel/installation' },
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
          '/ru/docs/': [
            { text: 'Установка', link: '/ru/docs/installation' },
            { text: 'Конфигурация', link: '/ru/docs/configuration' },
            { text: 'Архитектура', link: '/ru/docs/architecture' },
            { text: 'Топики', link: '/ru/docs/topics' },
            {
              text: 'Producer',
              items: [
                { text: 'Отправка сообщений', link: '/ru/docs/producer/producing' },
                { text: 'Настройка', link: '/ru/docs/producer/configure' },
                { text: 'Pipeline', link: '/ru/docs/producer/pipeline' },
              ],
            },
            {
              text: 'Consumer',
              items: [
                { text: 'Получение сообщений', link: '/ru/docs/consumer/consuming' },
                { text: 'Настройка', link: '/ru/docs/consumer/configure' },
                { text: 'Pipeline', link: '/ru/docs/consumer/pipeline' },
              ],
            },
            {
              text: 'Компоненты',
              items: [
                { text: 'Messages', link: '/ru/docs/components/messages' },
                { text: 'Commiter', link: '/ru/docs/components/commiter' },
              ],
            },
            {
              text: 'Laravel',
              items: [
                { text: 'Установка', link: '/ru/docs/laravel/installation' },
                { text: 'Конфигурация', link: '/ru/docs/laravel/configuration' },
                { text: 'Отправка', link: '/ru/docs/laravel/producers' },
                { text: 'Получение', link: '/ru/docs/laravel/consumers' },
                { text: 'Команды', link: '/ru/docs/laravel/commands' },
                { text: 'Тестирование', link: '/ru/docs/laravel/testing' },
              ],
            },
          ],
        },

        editLink: {
          pattern: 'https://github.com/kafka-bus/kafka-bus.github.io/edit/main/:path',
          text: 'Редактировать страницу',
        },
        lastUpdated: { text: 'Обновлено' },
        footer: {
          message: 'Лицензия MIT.',
          copyright: 'Copyright © 2024-present Kirill Popkov',
        },
      },
    },
  },

  themeConfig: {
    logo: '/logo.svg',
    socialLinks: [
      { icon: 'github', link: 'https://github.com/kafka-bus/kafka-bus' },
    ],
    search: {
      provider: 'local',
    },
  },
})
