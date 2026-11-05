<script setup lang="ts">
import { VCard, VPageHeader } from '@halo-dev/components'
import ChunkUploadPanel from '../components/ChunkUploadPanel.vue'
</script>

<template>
  <VPageHeader title="大文件分片上传（断点续传）" />
  <div class="page-container">
    <VCard>
      <ChunkUploadPanel />
    </VCard>
  </div>
</template>

<style scoped>
.page-container {
  padding: 16px;
}
</style>
