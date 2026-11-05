package io.github.wickhe4.chunkupload;

import io.github.wickhe4.chunkupload.service.ChunkUploadService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import run.halo.app.plugin.BasePlugin;
import run.halo.app.plugin.PluginContext;

@Slf4j
@Component
public class ChunkUploadPlugin extends BasePlugin {

    private final ChunkUploadService chunkUploadService;

    public ChunkUploadPlugin(PluginContext pluginContext, ChunkUploadService chunkUploadService) {
        super(pluginContext);
        this.chunkUploadService = chunkUploadService;
    }

    @Override
    public void start() {
        chunkUploadService.cleanupExpiredSessions();
        log.info("大文件分片上传插件已启动");
    }

    @Override
    public void stop() {
        chunkUploadService.cleanupExpiredSessions();
        log.info("大文件分片上传插件已停止");
    }

    @Override
    public void delete() {
        chunkUploadService.cleanupAllSessions();
        log.info("大文件分片上传插件已卸载并清理临时目录");
    }
}
