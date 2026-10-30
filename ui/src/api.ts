import { axiosInstance, type Attachment } from '@halo-dev/api-client'

export const API_BASE = '/apis/chunkupload.halo.run/v1alpha1/uploads'

export interface SelectOption {
  name: string
  displayName: string
}

export interface UploadOptionsResponse {
  policies: SelectOption[]
  groups: SelectOption[]
}

export interface InitUploadRequest {
  filename: string
  totalSize: number
  chunkSize: number
  lastModified: number
  mediaType?: string
  policyName: string
  groupName?: string
}

export interface InitUploadResponse {
  uploadId: string
  totalChunks: number
  chunkSize: number
  uploadedChunks: number[]
  resumed: boolean
}

export interface ChunkUploadResponse {
  uploadId: string
  chunkIndex: number
  uploadedChunks: number[]
}

export interface CompleteUploadResponse {
  uploadId: string
  attachmentName: string
  displayName: string
  permalink?: string
  mediaType?: string
  size: number
  attachment: Attachment
}

export const chunkUploadApi = {
  getOptions: () =>
    axiosInstance.get<UploadOptionsResponse>(`${API_BASE}/options`),

  init: (data: InitUploadRequest) =>
    axiosInstance.post<InitUploadResponse>(`${API_BASE}/init`, data),

  status: (uploadId: string) =>
    axiosInstance.get<InitUploadResponse>(`${API_BASE}/${uploadId}`),

  uploadChunk: (
    uploadId: string,
    index: number,
    blob: Blob,
    filename: string,
    signal?: AbortSignal,
    onProgress?: (loadedBytes: number) => void
  ) => {
    const form = new FormData()
    form.append('file', blob, `${filename}.part${index}`)
    return axiosInstance.post<ChunkUploadResponse>(
      `${API_BASE}/${uploadId}/chunks/${index}`,
      form,
      {
        signal,
        onUploadProgress: (evt) => {
          if (onProgress && typeof evt.loaded === 'number') {
            onProgress(Math.min(evt.loaded, blob.size))
          }
        },
      }
    )
  },

  complete: (uploadId: string) =>
    axiosInstance.post<CompleteUploadResponse>(`${API_BASE}/${uploadId}/complete`),

  cancel: (uploadId: string) =>
    axiosInstance.delete<void>(`${API_BASE}/${uploadId}`),
}
