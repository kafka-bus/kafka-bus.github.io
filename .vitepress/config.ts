import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'Kafka Bus',
  description: 'PHP client for Apache Kafka',

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
      text: 'Edit this page',
    },

    lastUpdated: {
      text: 'Updated',
    },
  },
})
