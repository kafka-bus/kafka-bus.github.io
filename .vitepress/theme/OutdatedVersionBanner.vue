<script setup lang="ts">
import { computed } from 'vue'
import { useData, useRoute, withBase } from 'vitepress'

const route = useRoute()
const { lang } = useData()

const isOutdated = computed(() => /\/docs\/1\.x\//.test(route.path))
const isRu = computed(() => lang.value === 'ru-RU')

const latestLink = computed(() => withBase(isRu.value ? '/ru/docs/2.x/installation' : '/docs/2.x/installation'))
const upgradeLink = computed(() => withBase(isRu.value ? '/ru/docs/2.x/upgrade' : '/docs/2.x/upgrade'))
</script>

<template>
  <div v-if="isOutdated" class="custom-block warning outdated-version-banner">
    <p v-if="isRu">
      Вы читаете документацию Kafka Bus <strong>1.x</strong>. Актуальная версия —
      <a :href="latestLink">2.x</a>; что изменилось, описано в
      <a :href="upgradeLink">руководстве по переходу</a>.
    </p>
    <p v-else>
      You're reading the docs for Kafka Bus <strong>1.x</strong>. The current version is
      <a :href="latestLink">2.x</a> — see the <a :href="upgradeLink">upgrade guide</a> for what changed.
    </p>
  </div>
</template>

<style scoped>
.outdated-version-banner {
  margin-bottom: 24px;
}
</style>
