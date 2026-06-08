<script setup lang="ts">
defineProps<{
  icon: string
  title: string
  details: string
  link: string
}>()
</script>

<template>
  <div class="feature-row">
    <div class="feature-info">
      <div class="feature-icon">{{ icon }}</div>
      <h3 class="feature-title">{{ title }}</h3>
      <p class="feature-details">{{ details }}</p>
      <a :href="link" class="feature-link">Documentation →</a>
    </div>
    <div class="feature-code">
      <slot />
    </div>
  </div>
</template>

<style scoped>
.feature-row {
  display: grid;
  grid-template-columns: 1fr minmax(0, 1.6fr);
  gap: 48px;
  align-items: start;
  padding: 48px 0;
  border-bottom: 1px solid var(--vp-c-divider);
}

.feature-row:last-child {
  border-bottom: none;
}

@media (max-width: 768px) {
  .feature-row {
    grid-template-columns: 1fr;
    gap: 24px;
    padding: 32px 0;
  }
}

.feature-icon {
  font-size: 32px;
  line-height: 1;
  margin-bottom: 12px;
}

.feature-title {
  font-size: 20px;
  font-weight: 700;
  color: var(--vp-c-text-1);
  margin: 0 0 12px;
  border: none !important;
  padding: 0 !important;
}

.feature-details {
  font-size: 14px;
  color: var(--vp-c-text-2);
  line-height: 1.7;
  margin: 0 0 16px;
}

.feature-link {
  display: inline-block;
  font-size: 13px;
  font-weight: 500;
  color: var(--vp-c-brand-1);
  text-decoration: none;
  transition: color 0.25s;
}

.feature-link:hover {
  color: var(--vp-c-brand-2);
}

.feature-code {
  min-width: 0;
}

.feature-code :deep(div[class*='language-']) {
  margin: 0 !important;
  border-radius: 10px;
}
</style>
