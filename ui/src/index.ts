import { definePlugin, type AttachmentSelectProvider } from '@halo-dev/ui-shared'
import { IconUpload } from '@halo-dev/components'
import { markRaw } from 'vue'
import ChunkUploadPanel from './components/ChunkUploadPanel.vue'
import ChunkUploadView from './views/ChunkUploadView.vue'

export default definePlugin({
  components: {},
  routes: [
    {
      parentName: 'Root',
      route: {
        path: '/chunk-upload',
        name: 'ChunkUpload',
        component: ChunkUploadView,
        meta: {
          title: '大文件分片上传',
          searchable: true,
          permissions: ['plugin:chunk-upload:manage'],
          menu: {
            name: '大文件分片上传',
            icon: markRaw(IconUpload),
            priority: 0,
          },
        },
      },
    },
  ],
  extensionPoints: {
    'attachment:selector:create': (): AttachmentSelectProvider[] => {
      return [
        {
          id: 'chunk-upload-selector',
          label: '大文件分片上传',
          component: markRaw(ChunkUploadPanel),
        },
      ]
    },
  },
})
