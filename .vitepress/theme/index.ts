import DefaultTheme from 'vitepress/theme'
import type { Theme } from 'vitepress'
import FeatureCard from './FeatureCard.vue'

export default {
  extends: DefaultTheme,
  enhanceApp({ app }) {
    app.component('FeatureCard', FeatureCard)
  },
} satisfies Theme
