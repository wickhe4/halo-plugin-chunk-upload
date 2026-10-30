<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { Toast, VButton } from '@halo-dev/components'
import type { AttachmentLike } from '@halo-dev/ui-shared'
import {
  chunkUploadApi,
  type CompleteUploadResponse,
  type SelectOption,
} from './../api'

const props = withDefaults(
  defineProps<{
    selected?: AttachmentLike[]
  }>(),
  {
    selected: () => [],
  }
)

const emit = defineEmits<{
  (event: 'update:selected', attachments: AttachmentLike[]): void
}>()

const policies = ref<SelectOption[]>([])
const groups = ref<SelectOption[]>([])
const policyName = ref('default-policy')
const groupName = ref('')
const chunkSizeMb = ref(5)
const concurrency = ref(2)

const file = ref<File | null>(null)
const fileInput = ref<HTMLInputElement | null>(null)
const dragOver = ref(false)

const currentUploadId = ref('')
const uploading = ref(false)
const paused = ref(false)
const merging = ref(false)
const totalChunks = ref(0)
const uploadedCount = ref(0)
const statusText = ref('')
const speedText = ref('')
const uploadedBytesTotal = ref(0)
const uploadedItems = ref<CompleteUploadResponse[]>([])
const selectedNames = ref<Set<string>>(new Set())

let abortController: AbortController | null = null

const chunkSizeBytes = computed(() => chunkSizeMb.value * 1024 * 1024)
const sessionLocked = computed(() => uploading.value || uploadedCount.value > 0 || !!currentUploadId.value)
const progressPercent = computed(() => {
  if (merging.value) return 98
  if (!file.value || file.value.size <= 0) return 0
  return Math.min(97, Math.round((uploadedBytesTotal.value / file.value.size) * 97))
})

onMounted(async () => {
  try {
    const { data } = await chunkUploadApi.getOptions()
    policies.value = data.policies || []
    groups.value = data.groups || []
    if (policies.value.length > 0 && !policies.value.some((p) => p.name === policyName.value)) {
      policyName.value = policies.value[0].name
    }
  } catch (err) {
    Toast.error('加载 Halo 存储策略列表失败')
  }
})

onBeforeUnmount(() => {
  if (uploading.value) {
    paused.value = true
    abortController?.abort()
  }
})

function triggerFileSelect() {
  if (uploading.value) return
  fileInput.value?.click()
}

function onFileChange(event: Event) {
  const target = event.target as HTMLInputElement
  const picked = target.files?.[0] || null
  setSelectedFile(picked)
}

function onDrop(event: DragEvent) {
  dragOver.value = false
  if (uploading.value) return
  const picked = event.dataTransfer?.files?.[0] || null
  setSelectedFile(picked)
}

function setSelectedFile(picked: File | null) {
  if (picked && picked.size <= 0) {
    Toast.warning('文件大小为 0 字节，无法上传空文件')
    if (fileInput.value) fileInput.value.value = ''
    return
  }
  file.value = picked
  currentUploadId.value = ''
  paused.value = false
  merging.value = false
  uploadedCount.value = 0
  uploadedBytesTotal.value = 0
  speedText.value = ''
  totalChunks.value = picked ? Math.ceil(picked.size / chunkSizeBytes.value) : 0
  statusText.value = picked ? `准备就绪，共 ${totalChunks.value} 个分片` : ''
}

function chunkActualSize(totalSize: number, cSize: number, index: number, total: number) {
  if (index < total - 1) return cSize
  const rem = totalSize - (total - 1) * cSize
  return rem > 0 ? rem : cSize
}

async function startOrResumeUpload() {
  if (!file.value) {
    Toast.warning('请先选择要上传的文件')
    return
  }
  if (!policyName.value) {
    Toast.warning('请选择目标存储策略')
    return
  }

  uploading.value = true
  paused.value = false
  merging.value = false
  speedText.value = ''
  abortController = new AbortController()

  try {
    const selectedFile = file.value
    const { data: initData } = await chunkUploadApi.init({
      filename: selectedFile.name,
      totalSize: selectedFile.size,
      chunkSize: chunkSizeBytes.value,
      lastModified: selectedFile.lastModified,
      mediaType: selectedFile.type || undefined,
      policyName: policyName.value,
      groupName: groupName.value || undefined,
    })

    currentUploadId.value = initData.uploadId
    totalChunks.value = initData.totalChunks
    const uploadedSet = new Set<number>(initData.uploadedChunks || [])
    uploadedCount.value = uploadedSet.size

    // 计算已完成分片的字节数与正在上传分片的实时字节数
    let completedBytes = 0
    const pendingIndices: number[] = []
    for (let i = 0; i < initData.totalChunks; i++) {
      if (uploadedSet.has(i)) {
        completedBytes += chunkActualSize(selectedFile.size, initData.chunkSize, i, initData.totalChunks)
      } else {
        pendingIndices.push(i)
      }
    }
    const inFlightBytes = new Map<number, number>()
    uploadedBytesTotal.value = completedBytes

    const sessionStartTime = performance.now()
    const sessionStartBytes = completedBytes

    const refreshRealtimeProgress = () => {
      let inFlightSum = 0
      inFlightBytes.forEach((b) => {
        inFlightSum += b
      })
      const currentTotal = Math.min(selectedFile.size, completedBytes + inFlightSum)
      uploadedBytesTotal.value = currentTotal

      const elapsedSec = (performance.now() - sessionStartTime) / 1000
      const deltaBytes = currentTotal - sessionStartBytes
      if (elapsedSec > 0.3 && deltaBytes > 0) {
        const bps = deltaBytes / elapsedSec
        const remainingBytes = Math.max(0, selectedFile.size - currentTotal)
        const etaSec = bps > 0 ? Math.ceil(remainingBytes / bps) : 0
        speedText.value = `${formatSize(bps)}/s · 预计剩余 ${formatDuration(etaSec)}`
      }
    }

    if (initData.resumed && uploadedSet.size > 0) {
      Toast.info(`已自动恢复断点续传：跳过已上传的 ${uploadedSet.size}/${initData.totalChunks} 个分片`)
    }

    const workerCount = Math.max(1, Math.min(concurrency.value, pendingIndices.length || 1))
    statusText.value = `正在并发上传（${workerCount} 线程）...`

    let cursor = 0
    let firstError: any = null

    const runWorker = async () => {
      while (!paused.value && !firstError) {
        const currentPos = cursor++
        if (currentPos >= pendingIndices.length) {
          return
        }
        const index = pendingIndices[currentPos]
        const start = index * initData.chunkSize
        const end = Math.min(start + initData.chunkSize, selectedFile.size)
        const chunkBlob = selectedFile.slice(start, end)

        try {
          await uploadChunkWithRetry(
            initData.uploadId,
            index,
            chunkBlob,
            selectedFile.name,
            abortController!.signal,
            (loaded) => {
              inFlightBytes.set(index, loaded)
              refreshRealtimeProgress()
            }
          )
          inFlightBytes.delete(index)
          completedBytes += chunkBlob.size
          uploadedSet.add(index)
          uploadedCount.value = uploadedSet.size
          refreshRealtimeProgress()
        } catch (err: any) {
          inFlightBytes.delete(index)
          if (!paused.value && !firstError) {
            firstError = err
            abortController?.abort()
          }
          return
        }
      }
    }

    if (pendingIndices.length > 0) {
      await Promise.all(Array.from({ length: workerCount }, () => runWorker()))
    }

    if (paused.value) {
      speedText.value = ''
      statusText.value = `已暂停（已完成 ${uploadedCount.value} / ${initData.totalChunks} 片），点击「继续上传」可从断点恢复`
      return
    }

    if (firstError) {
      throw firstError
    }

    merging.value = true
    speedText.value = ''
    statusText.value = '全部分片上传完成，服务端正在零拷贝高速合并并写入 Halo 附件库...'
    const { data: completed } = await chunkUploadApi.complete(initData.uploadId)

    uploadedItems.value.unshift(completed)
    toggleAttachmentSelection(completed, true)
    Toast.success(`上传成功：${completed.displayName}`)

    // 重置当前文件状态
    file.value = null
    if (fileInput.value) fileInput.value.value = ''
    currentUploadId.value = ''
    statusText.value = ''
    speedText.value = ''
    uploadedCount.value = 0
    uploadedBytesTotal.value = 0
    totalChunks.value = 0
  } catch (err: any) {
    speedText.value = ''
    if (paused.value || err?.name === 'CanceledError' || err?.code === 'ERR_CANCELED') {
      statusText.value = `已暂停（已完成 ${uploadedCount.value} / ${totalChunks.value} 片），点击「继续上传」可从断点恢复`
    } else {
      const msg = err?.response?.data?.detail || err?.response?.data?.message || err?.message || '分片上传失败'
      statusText.value = `上传中断：${msg}（点击「继续上传」可从当前分片恢复）`
      Toast.error(msg)
    }
  } finally {
    uploading.value = false
    merging.value = false
  }
}

async function uploadChunkWithRetry(
  uploadId: string,
  index: number,
  blob: Blob,
  filename: string,
  signal: AbortSignal,
  onProgress: (loaded: number) => void,
  maxRetries = 3
) {
  let attempt = 0
  while (true) {
    try {
      await chunkUploadApi.uploadChunk(uploadId, index, blob, filename, signal, onProgress)
      return
    } catch (err: any) {
      if (paused.value || signal.aborted || err?.name === 'CanceledError' || err?.code === 'ERR_CANCELED') {
        throw err
      }
      attempt++
      if (attempt >= maxRetries) {
        throw err
      }
      onProgress(0)
      statusText.value = `分片 ${index + 1} 网络波动，正在进行第 ${attempt} 次自动重试...`
      await new Promise((resolve) => setTimeout(resolve, 800 * attempt))
    }
  }
}

function pauseUpload() {
  if (!uploading.value) return
  paused.value = true
  abortController?.abort()
}

async function cancelAndCleanUpload() {
  paused.value = true
  abortController?.abort()
  if (currentUploadId.value) {
    try {
      await chunkUploadApi.cancel(currentUploadId.value)
      Toast.info('已取消上传并清理服务端临时分片')
    } catch {
      // ignore
    }
  }
  file.value = null
  if (fileInput.value) fileInput.value.value = ''
  currentUploadId.value = ''
  uploadedCount.value = 0
  uploadedBytesTotal.value = 0
  totalChunks.value = 0
  statusText.value = ''
  speedText.value = ''
}

function toggleAttachmentSelection(item: CompleteUploadResponse, forceSelect = false) {
  const name = item.attachmentName
  if (forceSelect) {
    selectedNames.value.add(name)
  } else if (selectedNames.value.has(name)) {
    selectedNames.value.delete(name)
  } else {
    selectedNames.value.add(name)
  }

  const selectedAttachments: AttachmentLike[] = uploadedItems.value
    .filter((i) => selectedNames.value.has(i.attachmentName))
    .map((i) => i.attachment)

  emit('update:selected', selectedAttachments)
}

async function copyPermalink(url?: string) {
  if (!url) {
    Toast.warning('该附件暂无直链')
    return
  }
  try {
    await navigator.clipboard.writeText(url)
    Toast.success('附件链接已复制到剪贴板')
  } catch {
    Toast.warning('复制失败，请手动复制')
  }
}

function formatSize(bytes?: number) {
  if (!bytes || bytes <= 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let val = bytes
  let idx = 0
  while (val >= 1024 && idx < units.length - 1) {
    val /= 1024
    idx++
  }
  return `${val.toFixed(idx === 0 ? 0 : 2)} ${units[idx]}`
}

function formatDuration(seconds: number) {
  if (seconds <= 0) return '1 秒'
  if (seconds < 60) return `${seconds} 秒`
  const mins = Math.floor(seconds / 60)
  const secs = seconds % 60
  return `${mins} 分 ${secs} 秒`
}
</script>

<template>
  <div class="chunk-upload-wrapper">
    <!-- 顶部配置栏：存储策略、分组、分片大小、并发数 -->
    <div class="config-grid">
      <div class="field-item">
        <label class="field-label">存储策略 (Policy)</label>
        <select v-model="policyName" class="field-select" :disabled="sessionLocked">
          <option v-for="p in policies" :key="p.name" :value="p.name">
            {{ p.displayName }}
          </option>
        </select>
      </div>

      <div class="field-item">
        <label class="field-label">附件分组 (Group)</label>
        <select v-model="groupName" class="field-select" :disabled="sessionLocked">
          <option value="">未分组 (默认)</option>
          <option v-for="g in groups" :key="g.name" :value="g.name">
            {{ g.displayName }}
          </option>
        </select>
      </div>

      <div class="field-item">
        <label class="field-label">单次分片大小 (绕过 CF 100MB)</label>
        <select
          v-model.number="chunkSizeMb"
          class="field-select"
          :disabled="sessionLocked"
          @change="file && setSelectedFile(file)"
        >
          <option :value="2">2 MB / 片（弱网推荐）</option>
          <option :value="5">5 MB / 片（默认推荐）</option>
          <option :value="10">10 MB / 片（高速推荐）</option>
          <option :value="20">20 MB / 片（大文件推荐）</option>
          <option :value="50">50 MB / 片（上限 < 100MB）</option>
        </select>
      </div>

      <div class="field-item">
        <label class="field-label">并发上传线程数</label>
        <select v-model.number="concurrency" class="field-select" :disabled="uploading">
          <option :value="1">1 线程</option>
          <option :value="2">2 线程（默认推荐）</option>
          <option :value="4">4 线程</option>
          <option :value="6">6 线程</option>
          <option :value="8">8 线程</option>
          <option :value="10">10 线程</option>
          <option :value="12">12 线程</option>
        </select>
      </div>
    </div>

    <!-- 拖拽/选择文件区 -->
    <input
      ref="fileInput"
      type="file"
      class="hidden-input"
      :disabled="uploading"
      @change="onFileChange"
    />

    <div
      class="drop-zone"
      :class="{ 'is-dragover': dragOver, 'has-file': !!file }"
      @click="triggerFileSelect"
      @dragover.prevent="dragOver = true"
      @dragleave.prevent="dragOver = false"
      @drop.prevent="onDrop"
    >
      <div v-if="!file" class="drop-empty">
        <div class="drop-title">点击选择或拖拽大文件到此区域</div>
        <div class="drop-subtitle">
          浏览器端自动按 {{ chunkSizeMb }}MB 切片 · {{ concurrency }} 线程并发上传 · 支持断点续传 · 零拷贝高速合并入库
        </div>
      </div>

      <div v-else class="file-summary" @click.stop>
        <div class="file-info">
          <div class="file-name">{{ file.name }}</div>
          <div class="file-meta">
            总大小：{{ formatSize(file.size) }} · 已传：{{ formatSize(uploadedBytesTotal) }} · 分片：{{ chunkSizeMb }} MB × {{ totalChunks }} 片
          </div>
          <div v-if="statusText || speedText" class="file-status">
            <span>{{ statusText }}</span>
            <span v-if="speedText" class="speed-badge">{{ speedText }}</span>
          </div>
        </div>

        <div class="file-actions">
          <VButton
            v-if="!uploading"
            type="primary"
            @click="startOrResumeUpload"
          >
            {{ uploadedCount > 0 ? '继续上传 (断点续传)' : '开始分片上传' }}
          </VButton>
          <VButton
            v-if="uploading && !merging"
            type="default"
            @click="pauseUpload"
          >
            暂停
          </VButton>
          <VButton
            v-if="!uploading"
            type="default"
            @click="triggerFileSelect"
          >
            更换文件
          </VButton>
          <VButton
            v-if="file && !merging"
            type="danger"
            @click="cancelAndCleanUpload"
          >
            取消并清理
          </VButton>
        </div>
      </div>

      <!-- 进度条 -->
      <div v-if="file && (uploading || uploadedCount > 0 || uploadedBytesTotal > 0)" class="progress-track" @click.stop>
        <div class="progress-fill" :style="{ width: `${progressPercent}%` }" />
        <span class="progress-label">
          {{ progressPercent }}% (已完成 {{ uploadedCount }} / {{ totalChunks }} 分片)
        </span>
      </div>
    </div>

    <!-- 本次已上传并转入 Halo 附件库的文件列表 -->
    <div v-if="uploadedItems.length > 0" class="uploaded-section">
      <div class="section-header">
        <span>本次已存入 Halo 附件库（点击卡片可勾选插入文章）</span>
      </div>
      <div class="uploaded-list">
        <div
          v-for="item in uploadedItems"
          :key="item.attachmentName"
          class="uploaded-card"
          :class="{ 'is-selected': selectedNames.has(item.attachmentName) }"
          @click="toggleAttachmentSelection(item)"
        >
          <div class="card-left">
            <input
              type="checkbox"
              :checked="selectedNames.has(item.attachmentName)"
              @click.stop="toggleAttachmentSelection(item)"
            />
            <div class="card-meta">
              <div class="card-title">{{ item.displayName }}</div>
              <div class="card-sub">
                {{ formatSize(item.size) }} · {{ item.mediaType || 'application/octet-stream' }}
                <span v-if="item.permalink"> · {{ item.permalink }}</span>
              </div>
            </div>
          </div>
          <div class="card-right" @click.stop>
            <VButton size="sm" @click="copyPermalink(item.permalink)">复制链接</VButton>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.chunk-upload-wrapper {
  display: flex;
  flex-direction: column;
  gap: 16px;
  padding: 16px;
  color: #1f2937;
}

.config-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 12px;
}

.field-item {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.field-label {
  font-size: 13px;
  font-weight: 500;
  color: #374151;
}

.field-select {
  height: 36px;
  padding: 0 10px;
  border: 1px solid #d1d5db;
  border-radius: 6px;
  background-color: #fff;
  font-size: 13px;
  color: #111827;
}

.field-select:disabled {
  background-color: #f3f4f6;
  color: #9ca3af;
  cursor: not-allowed;
}

.hidden-input {
  display: none;
}

.drop-zone {
  display: flex;
  flex-direction: column;
  gap: 14px;
  padding: 28px 20px;
  border: 2px dashed #cbd5e1;
  border-radius: 8px;
  background-color: #f8fafc;
  cursor: pointer;
  transition: all 0.2s ease;
}

.drop-zone:hover,
.drop-zone.is-dragover,
.drop-zone.has-file {
  border-color: #2563eb;
  background-color: #eff6ff;
}

.drop-empty {
  text-align: center;
}

.drop-title {
  font-size: 15px;
  font-weight: 600;
  color: #1e293b;
}

.drop-subtitle {
  margin-top: 6px;
  font-size: 12px;
  color: #64748b;
}

.file-summary {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  cursor: default;
}

.file-info {
  min-width: 0;
  flex: 1;
}

.file-name {
  font-size: 14px;
  font-weight: 600;
  color: #0f172a;
  word-break: break-all;
}

.file-meta {
  margin-top: 4px;
  font-size: 12px;
  color: #475569;
}

.file-status {
  margin-top: 6px;
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 8px;
  font-size: 12px;
  font-weight: 500;
  color: #2563eb;
}

.speed-badge {
  display: inline-block;
  padding: 1px 8px;
  border-radius: 999px;
  background-color: #dbeafe;
  color: #1d4ed8;
  font-weight: 600;
}

.file-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.progress-track {
  position: relative;
  height: 22px;
  border-radius: 999px;
  background-color: #dbeafe;
  overflow: hidden;
  cursor: default;
}

.progress-fill {
  height: 100%;
  background-color: #2563eb;
  transition: width 0.15s linear;
}

.progress-label {
  position: absolute;
  inset: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 12px;
  font-weight: 600;
  color: #0f172a;
}

.uploaded-section {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.section-header {
  font-size: 13px;
  font-weight: 600;
  color: #374151;
}

.uploaded-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.uploaded-card {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 10px 14px;
  border: 1px solid #e2e8f0;
  border-radius: 6px;
  background-color: #fff;
  cursor: pointer;
}

.uploaded-card.is-selected {
  border-color: #2563eb;
  background-color: #eff6ff;
}

.card-left {
  display: flex;
  align-items: center;
  gap: 10px;
  min-width: 0;
}

.card-meta {
  min-width: 0;
}

.card-title {
  font-size: 13px;
  font-weight: 600;
  color: #1e293b;
  word-break: break-all;
}

.card-sub {
  margin-top: 2px;
  font-size: 12px;
  color: #64748b;
  word-break: break-all;
}
</style>
