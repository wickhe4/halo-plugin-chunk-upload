package run.halo.chunkupload.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import run.halo.app.core.extension.attachment.Attachment;
import run.halo.app.core.extension.attachment.Attachment.AttachmentSpec;
import run.halo.app.core.extension.attachment.Policy;
import run.halo.app.core.extension.service.AttachmentService;
import run.halo.app.extension.ExtensionClient;
import run.halo.app.extension.Metadata;
import run.halo.chunkupload.dto.ChunkUploadDtos.CompleteUploadResponse;
import run.halo.chunkupload.dto.ChunkUploadDtos.InitUploadRequest;
import run.halo.chunkupload.dto.ChunkUploadDtos.InitUploadResponse;

class ChunkUploadServiceTest {

    @TempDir
    Path tempDir;

    private AttachmentService attachmentService;
    private ExtensionClient extensionClient;
    private ChunkUploadService chunkUploadService;

    @BeforeEach
    void setUp() {
        attachmentService = mock(AttachmentService.class);
        extensionClient = mock(ExtensionClient.class);

        Policy defaultPolicy = new Policy();
        Metadata metadata = new Metadata();
        metadata.setName("default-policy");
        defaultPolicy.setMetadata(metadata);
        when(extensionClient.fetch(eq(Policy.class), eq("default-policy")))
            .thenReturn(Optional.of(defaultPolicy));

        chunkUploadService = new ChunkUploadService(attachmentService, extensionClient, tempDir);
    }

    @Test
    void shouldRejectExcessiveTotalChunksToPreventDoS() {
        long chunkSize = 1024L * 1024L; // 1 MB
        long excessiveTotalSize = chunkSize * (ChunkUploadService.MAX_TOTAL_CHUNKS + 1L);
        InitUploadRequest request = new InitUploadRequest(
            "huge.bin", excessiveTotalSize, chunkSize, 1000L, "application/octet-stream", "default-policy", ""
        );

        assertThrows(ResponseStatusException.class, () -> chunkUploadService.init(request));
    }

    @Test
    void shouldRejectInvalidUploadIdForPathTraversalPrevention() {
        assertThrows(ResponseStatusException.class, () -> chunkUploadService.status("../etc/passwd"));
        assertThrows(ResponseStatusException.class, () -> chunkUploadService.cancel("../../tmp"));
    }

    @Test
    void shouldInitUploadChunksAndMergeSuccessfully() throws Exception {
        long chunkSize = 512L * 1024L; // 512 KB
        byte[] chunk0Bytes = new byte[(int) chunkSize];
        java.util.Arrays.fill(chunk0Bytes, (byte) 'A');
        byte[] chunk1Bytes = "HaloChunkUploadTail".getBytes(StandardCharsets.UTF_8);
        long totalSize = chunk0Bytes.length + chunk1Bytes.length;

        InitUploadRequest initReq = new InitUploadRequest(
            "../test-video.mp4", totalSize, chunkSize, 123456L, "video/mp4", "default-policy", ""
        );
        InitUploadResponse initRes = chunkUploadService.init(initReq);

        assertNotNull(initRes.uploadId());
        assertEquals(2, initRes.totalChunks());
        assertTrue(initRes.uploadedChunks().isEmpty());
        assertFalse(initRes.resumed());

        // 上传分片 0 与分片 1
        FilePart part0 = mockFilePart(chunk0Bytes);
        chunkUploadService.uploadChunk(initRes.uploadId(), 0, part0).block();

        // 模拟断点续传重新 init，应识别已上传的分片 0
        InitUploadResponse resumedRes = chunkUploadService.init(initReq);
        assertTrue(resumedRes.resumed());
        assertEquals(Set.of(0), resumedRes.uploadedChunks());

        FilePart part1 = mockFilePart(chunk1Bytes);
        chunkUploadService.uploadChunk(initRes.uploadId(), 1, part1).block();

        // 模拟 Halo AttachmentService.upload
        when(attachmentService.upload(
                eq("default-policy"),
                eq(null),
                eq("test-video.mp4"),
                any(Flux.class),
                any(org.springframework.http.MediaType.class)))
            .thenAnswer(invocation -> {
                Flux<org.springframework.core.io.buffer.DataBuffer> content = invocation.getArgument(3);
                return DataBufferUtils.join(content).map(joined -> {
                    int mergedByteCount = joined.readableByteCount();
                    DataBufferUtils.release(joined);
                    assertEquals((int) totalSize, mergedByteCount);

                    Attachment att = new Attachment();
                    Metadata meta = new Metadata();
                    meta.setName("att-001");
                    att.setMetadata(meta);
                    AttachmentSpec spec = new AttachmentSpec();
                    spec.setDisplayName("test-video.mp4");
                    spec.setMediaType("video/mp4");
                    spec.setSize(totalSize);
                    att.setSpec(spec);
                    return att;
                });
            });
        when(attachmentService.getPermalink(any()))
            .thenReturn(Mono.just(URI.create("/upload/test-video.mp4")));

        CompleteUploadResponse completeRes = chunkUploadService.complete(initRes.uploadId()).block();
        assertNotNull(completeRes);
        assertEquals("att-001", completeRes.attachmentName());
        assertEquals("test-video.mp4", completeRes.displayName());
        assertEquals("/upload/test-video.mp4", completeRes.permalink());
        assertEquals(totalSize, completeRes.size());

        // 合并完成后会话临时目录应已被自动清理
        assertFalse(Files.exists(tempDir.resolve(initRes.uploadId())));
    }

    @Test
    void shouldRetainMergedFileAndSupportRetryWhenAttachmentUploadFails() throws Exception {
        long chunkSize = 512L * 1024L;
        byte[] chunk0Bytes = new byte[(int) chunkSize];
        java.util.Arrays.fill(chunk0Bytes, (byte) 'B');
        byte[] chunk1Bytes = "RetryTail".getBytes(StandardCharsets.UTF_8);
        long totalSize = chunk0Bytes.length + chunk1Bytes.length;

        InitUploadRequest initReq = new InitUploadRequest(
            "retry-video.mp4", totalSize, chunkSize, 654321L, "video/mp4", "default-policy", ""
        );
        InitUploadResponse initRes = chunkUploadService.init(initReq);

        chunkUploadService.uploadChunk(initRes.uploadId(), 0, mockFilePart(chunk0Bytes)).block();
        chunkUploadService.uploadChunk(initRes.uploadId(), 1, mockFilePart(chunk1Bytes)).block();

        // 第一次入库模拟失败（如存储策略单文件大小超限）
        when(attachmentService.upload(
                eq("default-policy"),
                eq(null),
                eq("retry-video.mp4"),
                any(Flux.class),
                any(org.springframework.http.MediaType.class)))
            .thenReturn(Mono.error(new ResponseStatusException(
                org.springframework.http.HttpStatus.BAD_REQUEST, "文件大小超过存储策略限制")));

        assertThrows(ResponseStatusException.class,
            () -> chunkUploadService.complete(initRes.uploadId()).block());

        // 此时重新 init 应仍识别出全部分片已完成（基于已保留的 mergedFile）
        InitUploadResponse resumedAfterMergeFail = chunkUploadService.init(initReq);
        assertTrue(resumedAfterMergeFail.resumed());
        assertEquals(Set.of(0, 1), resumedAfterMergeFail.uploadedChunks());

        // 修复存储策略后第二次调用 complete，应直接复用已合并文件成功入库
        when(attachmentService.upload(
                eq("default-policy"),
                eq(null),
                eq("retry-video.mp4"),
                any(Flux.class),
                any(org.springframework.http.MediaType.class)))
            .thenAnswer(invocation -> {
                Flux<org.springframework.core.io.buffer.DataBuffer> content = invocation.getArgument(3);
                return DataBufferUtils.join(content).map(joined -> {
                    int mergedByteCount = joined.readableByteCount();
                    DataBufferUtils.release(joined);
                    assertEquals((int) totalSize, mergedByteCount);

                    Attachment att = new Attachment();
                    Metadata meta = new Metadata();
                    meta.setName("att-retry-002");
                    att.setMetadata(meta);
                    AttachmentSpec spec = new AttachmentSpec();
                    spec.setDisplayName("retry-video.mp4");
                    spec.setMediaType("video/mp4");
                    spec.setSize(totalSize);
                    att.setSpec(spec);
                    return att;
                });
            });
        when(attachmentService.getPermalink(any()))
            .thenReturn(Mono.just(URI.create("/upload/retry-video.mp4")));

        CompleteUploadResponse secondTry = chunkUploadService.complete(initRes.uploadId()).block();
        assertNotNull(secondTry);
        assertEquals("att-retry-002", secondTry.attachmentName());
        assertFalse(Files.exists(tempDir.resolve(initRes.uploadId())));
    }

    @Test
    void shouldSanitizeCrossPlatformAndInvalidFilenames() {
        assertEquals("video_test.mp4", chunkUploadService.sanitizeFilename("C:\\fakepath\\video:test.mp4"));
        assertEquals("archive.zip", chunkUploadService.sanitizeFilename("../../archive.zip"));
        assertThrows(ResponseStatusException.class, () -> chunkUploadService.sanitizeFilename("/"));
        assertThrows(ResponseStatusException.class, () -> chunkUploadService.sanitizeFilename(".."));
    }

    private FilePart mockFilePart(byte[] bytes) {
        FilePart filePart = mock(FilePart.class);
        when(filePart.transferTo(any(Path.class))).thenAnswer(invocation -> {
            Path dest = invocation.getArgument(0);
            Files.write(dest, bytes);
            return Mono.empty();
        });
        return filePart;
    }
}
