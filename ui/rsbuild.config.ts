import { rsbuildConfig } from '@halo-dev/ui-plugin-bundler-kit/rsbuild'
import { fileURLToPath, URL } from 'node:url'

export default rsbuildConfig({
  rsbuild: {
    tools: {
      rspack: {
        resolve: {
          alias: {
            '@': fileURLToPath(new URL('./src', import.meta.url)),
          },
        },
      },
    },
  },
})
