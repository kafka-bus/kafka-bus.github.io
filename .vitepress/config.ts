import { defineConfig, type DefaultTheme } from 'vitepress'

const LATEST = '2.x'

type Version = '1.x' | '2.x'

const enSidebar = (version: Version): DefaultTheme.SidebarItem[] => {
  const base = `/docs/${version}`

  const consumer: DefaultTheme.SidebarItem[] = version === '1.x'
    ? [
        { text: 'Consuming', link: `${base}/consumer/consuming` },
        { text: 'Configure', link: `${base}/consumer/configure` },
        { text: 'Pipeline', link: `${base}/consumer/pipeline` },
        { text: 'Testing', link: `${base}/consumer/testing` },
      ]
    : [
        { text: 'Consuming', link: `${base}/consumer/consuming` },
        { text: 'Routes', link: `${base}/consumer/configure` },
        { text: 'Workers', link: `${base}/consumer/workers` },
        { text: 'Pipeline', link: `${base}/consumer/pipeline` },
        { text: 'Testing', link: `${base}/consumer/testing` },
      ]

  const components: DefaultTheme.SidebarItem[] = [
    { text: 'Messages', link: `${base}/components/messages` },
    { text: 'Commiter', link: `${base}/components/commiter` },
    ...(version === '2.x' ? [{ text: 'Metadata', link: `${base}/components/metadata` }] : []),
  ]

  return [
    {
      text: `Kafka Bus ${version}`,
      items: [
        { text: 'Installation', link: `${base}/installation` },
        ...(version === '2.x' ? [{ text: 'Upgrading from 1.x', link: `${base}/upgrade` }] : []),
        { text: 'Configuration', link: `${base}/configuration` },
        { text: 'Architecture', link: `${base}/architecture` },
        { text: 'Topics', link: `${base}/topics` },
      ],
    },
    {
      text: 'Producer',
      items: [
        { text: 'Producing', link: `${base}/producer/producing` },
        { text: 'Configure', link: `${base}/producer/configure` },
        { text: 'Pipeline', link: `${base}/producer/pipeline` },
        { text: 'Testing', link: `${base}/producer/testing` },
      ],
    },
    { text: 'Consumer', items: consumer },
    { text: 'Components', items: components },
    {
      text: 'Laravel',
      items: [
        { text: 'Installation', link: `${base}/laravel/installation` },
        { text: 'Configuration', link: `${base}/laravel/configuration` },
        { text: 'Producing', link: `${base}/laravel/producers` },
        { text: 'Consuming', link: `${base}/laravel/consumers` },
        { text: 'Commands', link: `${base}/laravel/commands` },
        { text: 'Octane', link: `${base}/laravel/octane` },
        { text: 'Testing', link: `${base}/laravel/testing` },
      ],
    },
  ]
}

const ruSidebar = (version: Version): DefaultTheme.SidebarItem[] => {
  const base = `/ru/docs/${version}`

  const consumer: DefaultTheme.SidebarItem[] = version === '1.x'
    ? [
        { text: 'Получение', link: `${base}/consumer/consuming` },
        { text: 'Настройка', link: `${base}/consumer/configure` },
        { text: 'Pipeline', link: `${base}/consumer/pipeline` },
        { text: 'Тестирование', link: `${base}/consumer/testing` },
      ]
    : [
        { text: 'Получение', link: `${base}/consumer/consuming` },
        { text: 'Маршруты', link: `${base}/consumer/configure` },
        { text: 'Воркеры', link: `${base}/consumer/workers` },
        { text: 'Pipeline', link: `${base}/consumer/pipeline` },
        { text: 'Тестирование', link: `${base}/consumer/testing` },
      ]

  const components: DefaultTheme.SidebarItem[] = [
    { text: 'Messages', link: `${base}/components/messages` },
    { text: 'Commiter', link: `${base}/components/commiter` },
    ...(version === '2.x' ? [{ text: 'Metadata', link: `${base}/components/metadata` }] : []),
  ]

  return [
    {
      text: `Kafka Bus ${version}`,
      items: [
        { text: 'Установка', link: `${base}/installation` },
        ...(version === '2.x' ? [{ text: 'Переход с 1.x', link: `${base}/upgrade` }] : []),
        { text: 'Конфигурация', link: `${base}/configuration` },
        { text: 'Архитектура', link: `${base}/architecture` },
        { text: 'Топики', link: `${base}/topics` },
      ],
    },
    {
      text: 'Продюсер',
      items: [
        { text: 'Отправка', link: `${base}/producer/producing` },
        { text: 'Настройка', link: `${base}/producer/configure` },
        { text: 'Pipeline', link: `${base}/producer/pipeline` },
        { text: 'Тестирование', link: `${base}/producer/testing` },
      ],
    },
    { text: 'Консьюмер', items: consumer },
    { text: 'Компоненты', items: components },
    {
      text: 'Laravel',
      items: [
        { text: 'Установка', link: `${base}/laravel/installation` },
        { text: 'Конфигурация', link: `${base}/laravel/configuration` },
        { text: 'Отправка', link: `${base}/laravel/producers` },
        { text: 'Получение', link: `${base}/laravel/consumers` },
        { text: 'Команды', link: `${base}/laravel/commands` },
        { text: 'Octane', link: `${base}/laravel/octane` },
        { text: 'Тестирование', link: `${base}/laravel/testing` },
      ],
    },
  ]
}

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
          { text: 'Docs', link: `/docs/${LATEST}/installation`, activeMatch: '^/docs/' },
          {
            text: 'Components',
            items: [
              { text: 'Messages', link: `/docs/${LATEST}/components/messages` },
              { text: 'Commiter', link: `/docs/${LATEST}/components/commiter` },
              { text: 'Metadata', link: `/docs/${LATEST}/components/metadata` },
            ],
          },
          {
            text: 'Integrations',
            items: [
              { text: 'Laravel', link: `/docs/${LATEST}/laravel/installation` },
            ],
          },
          {
            text: 'Versions',
            items: [
              { text: '2.x (latest)', link: '/docs/2.x/installation', activeMatch: '^/docs/2\\.x/' },
              { text: '1.x', link: '/docs/1.x/installation', activeMatch: '^/docs/1\\.x/' },
              { text: 'Upgrading from 1.x', link: '/docs/2.x/upgrade' },
              { text: 'Changelog', link: 'https://github.com/kafka-bus/kafka-bus/blob/2.x/CHANGELOG.md' },
            ],
          },
        ],

        sidebar: {
          '/docs/2.x/': enSidebar('2.x'),
          '/docs/1.x/': enSidebar('1.x'),
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
          { text: 'Документация', link: `/ru/docs/${LATEST}/installation`, activeMatch: '^/ru/docs/' },
          {
            text: 'Компоненты',
            items: [
              { text: 'Messages', link: `/ru/docs/${LATEST}/components/messages` },
              { text: 'Commiter', link: `/ru/docs/${LATEST}/components/commiter` },
              { text: 'Metadata', link: `/ru/docs/${LATEST}/components/metadata` },
            ],
          },
          {
            text: 'Интеграции',
            items: [
              { text: 'Laravel', link: `/ru/docs/${LATEST}/laravel/installation` },
            ],
          },
          {
            text: 'Версии',
            items: [
              { text: '2.x (актуальная)', link: '/ru/docs/2.x/installation', activeMatch: '^/ru/docs/2\\.x/' },
              { text: '1.x', link: '/ru/docs/1.x/installation', activeMatch: '^/ru/docs/1\\.x/' },
              { text: 'Переход с 1.x', link: '/ru/docs/2.x/upgrade' },
              { text: 'Changelog', link: 'https://github.com/kafka-bus/kafka-bus/blob/2.x/CHANGELOG.md' },
            ],
          },
        ],

        sidebar: {
          '/ru/docs/2.x/': ruSidebar('2.x'),
          '/ru/docs/1.x/': ruSidebar('1.x'),
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
