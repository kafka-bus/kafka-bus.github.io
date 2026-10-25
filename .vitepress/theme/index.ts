import { h } from 'vue'
import DefaultTheme from 'vitepress/theme'
import type { Theme } from 'vitepress'
import FeatureCard from './FeatureCard.vue'
import OutdatedVersionBanner from './OutdatedVersionBanner.vue'

export default {
  extends: DefaultTheme,
  Layout: () => h(DefaultTheme.Layout, null, {
    'doc-before': () => h(OutdatedVersionBanner),
  }),
  enhanceApp({ app }) {
    app.component('FeatureCard', FeatureCard)
  },
} satisfies Theme
