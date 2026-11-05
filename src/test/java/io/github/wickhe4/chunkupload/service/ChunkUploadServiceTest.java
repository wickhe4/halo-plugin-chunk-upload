package io.github.wickhe4.chunkupload.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.wickhe4.chunkupload.dto.ChunkUploadDtos.CompleteUploadResponse;
import io.github.wickhe4.chunkupload.dto.ChunkUploadDtos.InitUploadRequest;
import io.github.wickhe4.chunkupload.dto.ChunkUploadDtos.InitUploadResponse;
import io.github.wickhe4.chunkupload.dto.ChunkUploadDtos.UploadOptionsResponse;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;
import run.halo.app.core.extension.attachment.Attachment;
import run.halo.app.core.extension.attachment.Attachment.AttachmentSpec;
import run.halo.app.core.extension.attachment.Group;
import run.halo.app.core.extension.attachment.Group.GroupSpec;
import run.halo.app.core.extension.attachment.Policy;
import run.halo.app.core.extension.attachment.Policy.PolicySpec;
import run.halo.app.core.extension.service.AttachmentService;
import run.halo.app.extension.Metadata;
import run.halo.app.extension.ReactiveExtensionClient;

class ChunkUploadServiceTest {

    @TempDir
    Path tempDir;

    private AttachmentService attachmentService;
    private ReactiveExtensionClient extensionClient;
    private ChunkUploadService chunkUploadService;

    @BeforeEach
    void setUp() {
        attachmentService = mock(AttachmentService.class);
        extensionClient = mock(ReactiveExtensionClient.class);

        Policy defaultPolicy = new Policy();
        Metadata metadata = new Metadata();
        metadata.setName("default-policy");
        defaultPolicy.setMetadata(metadata);
        PolicySpec policySpec = new PolicySpec();
        policySpec.setDisplayName("默认本地存储");
        defaultPolicy.setSpec(policySpec);

        when(extensionClient.fetch(eq(Policy.class), eq("default-policy")))
            .thenReturn(Mono.just(defaultPolicy));

        chunkUploadService = new ChunkUploadService(attachmentService, extensionClient, tempDir);
    }

    private Context withUser(String username) {
        var auth = new UsernamePasswordAuthenticationToken(username, "N/A", List.of());
        return ReactiveSecurityContextHolder.withAuthentication(auth);
    }

    @Test
    void shouldReturnAvailableUploadOptionsReactively() {
        Policy policy = new Policy();
        Metadata pMeta = new Metadata();
        pMeta.setName("default-policy");
        policy.setMetadata(pMeta);
        PolicySpec pSpec = new PolicySpec();
        pSpec.setDisplayName("本地存储");
        policy.setSpec(pSpec);

        Group group = new Group();
        Metadata gMeta = new Metadata();
        gMeta.setName("group-video");
        group.setMetadata(gMeta);
        GroupSpec gSpec = new GroupSpec();
        gSpec.setDisplayName("视频分组");
        group.setSpec(gSpec);

        when(extensionClient.list(eq(Policy.class), any(), any()))
            .thenReturn(Flux.just(policy));
        when(extensionClient.list(eq(Group.class), any(), any()))
            .thenReturn(Flux.just(group));

        UploadOptionsResponse options = chunkUploadService.getUploadOptions().block();
        assertNotNull(options);
        assertEquals(1, options.policies().size());
        assertEquals("本地存储", options.policies().get(0).displayName());
        assertEquals(1, options.groups().size());
        assertEquals("视频分组", options.groups().get(0).displayName());
    }

    @Test
    void shouldRejectExcessiveTotalChunksToPreventDoS() {
        long chunkSize = 1024L * 1024L; // 1 MB
        long excessiveTotalSize = chunkSize * (ChunkUploadService.MAX_TOTAL_CHUNKS + 1L);
        InitUploadRequest request = new InitUploadRequest(
            "huge.bin", excessiveTotalSize, chunkSize, 1000L, "application/octet-stream", "default-policy", ""
        );

        assertThrows(ResponseStatusException.class,
            () -> chunkUploadService.init(request).contextWrite(withUser("admin")).block());
    }

    @Test
    void shouldRejectInvalidUploadIdForPathTraversalPrevention() {
        assertThrows(ResponseStatusException.class,
            () -> chunkUploadService.status("../etc/passwd").contextWrite(withUser("admin")).block());
        assertThrows(ResponseStatusException.class,
            () -> chunkUploadService.cancel("../../tmp").contextWrite(withUser("admin")).block());
    }

    @Test
    void shouldIsolateSessionsBetweenUsersAndRejectCrossUserAccess() {
        long chunkSize = 512L * 1024L;
        InitUploadRequest req = new InitUploadRequest(
            "shared-name.mp4", 1024L * 1024L, chunkSize, 123456L, "video/mp4", "default-policy", ""
        );

        InitUploadResponse aliceRes = chunkUploadService.init(req).contextWrite(withUser("alice")).block();
        InitUploadResponse bobRes = chunkUploadService.init(req).contextWrite(withUser("bob")).block();

        assertNotNull(aliceRes);
        assertNotNull(bobRes);
        assertFalse(aliceRes.uploadId().equals(bobRes.uploadId()));

        ResponseStatusException statusEx = assertThrows(ResponseStatusException.class,
            () -> chunkUploadService.status(aliceRes.uploadId()).contextWrite(withUser("bob")).block());
        assertEquals(HttpStatus.FORBIDDEN, statusEx.getStatusCode());

        ResponseStatusException cancelEx = assertThrows(ResponseStatusException.class,
            () -> chunkUploadService.cancel(aliceRes.uploadId()).contextWrite(withUser("bob")).block());
        assertEquals(HttpStatus.FORBIDDEN, cancelEx.getStatusCode());
    }

    @Test
    void shouldRejectHiddenPolicyOrNonExistentGroup() {
        Policy hiddenPolicy = new Policy();
        Metadata hiddenMeta = new Metadata();
        hiddenMeta.setName("system-avatar-policy");
        hiddenMeta.setLabels(Map.of("halo.run/hidden", "true"));
        hiddenPolicy.setMetadata(hiddenMeta);
        when(extensionClient.fetch(eq(Policy.class), eq("system-avatar-policy")))
            .thenReturn(Mono.just(hiddenPolicy));

        InitUploadRequest hiddenPolicyReq = new InitUploadRequest(
            "a.mp4", 1024L, 512L * 1024L, 1L, "video/mp4", "system-avatar-policy", ""
        );
        assertThrows(ResponseStatusException.class,
            () -> chunkUploadService.init(hiddenPolicyReq).contextWrite(withUser("admin")).block());

        when(extensionClient.fetch(eq(Group.class), eq("missing-group")))
            .thenReturn(Mono.empty());
        InitUploadRequest missingGroupReq = new InitUploadRequest(
            "a.mp4", 1024L, 512L * 1024L, 1L, "video/mp4", "default-policy", "missing-group"
        );
        assertThrows(ResponseStatusException.class,
            () -> chunkUploadService.init(missingGroupReq).contextWrite(withUser("admin")).block());
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
        InitUploadResponse initRes = chunkUploadService.init(initReq).contextWrite(withUser("admin")).block();

        assertNotNull(initRes);
        assertNotNull(initRes.uploadId());
        assertEquals(2, initRes.totalChunks());
        assertTrue(initRes.uploadedChunks().isEmpty());
        assertFalse(initRes.resumed());

        // 上传分片 0 与分片 1
        FilePart part0 = mockFilePart(chunk0Bytes);
        chunkUploadService.uploadChunk(initRes.uploadId(), 0, part0).contextWrite(withUser("admin")).block();

        // 模拟断点续传重新 init，应识别已上传的分片 0
        InitUploadResponse resumedRes = chunkUploadService.init(initReq).contextWrite(withUser("admin")).block();
        assertNotNull(resumedRes);
        assertTrue(resumedRes.resumed());
        assertEquals(Set.of(0), resumedRes.uploadedChunks());

        FilePart part1 = mockFilePart(chunk1Bytes);
        chunkUploadService.uploadChunk(initRes.uploadId(), 1, part1).contextWrite(withUser("admin")).block();

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

        CompleteUploadResponse completeRes = chunkUploadService.complete(initRes.uploadId())
            .contextWrite(withUser("admin")).block();
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
        InitUploadResponse initRes = chunkUploadService.init(initReq).contextWrite(withUser("admin")).block();
        assertNotNull(initRes);

        chunkUploadService.uploadChunk(initRes.uploadId(), 0, mockFilePart(chunk0Bytes))
            .contextWrite(withUser("admin")).block();
        chunkUploadService.uploadChunk(initRes.uploadId(), 1, mockFilePart(chunk1Bytes))
            .contextWrite(withUser("admin")).block();

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
            () -> chunkUploadService.complete(initRes.uploadId()).contextWrite(withUser("admin")).block());

        // 此时重新 init 应仍识别出全部分片已完成（基于已保留的 mergedFile）
        InitUploadResponse resumedAfterMergeFail = chunkUploadService.init(initReq)
            .contextWrite(withUser("admin")).block();
        assertNotNull(resumedAfterMergeFail);
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

        CompleteUploadResponse secondTry = chunkUploadService.complete(initRes.uploadId())
            .contextWrite(withUser("admin")).block();
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
