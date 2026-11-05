package io.github.wickhe4.chunkupload.service;

import io.github.wickhe4.chunkupload.dto.ChunkUploadDtos.ChunkUploadResponse;
import io.github.wickhe4.chunkupload.dto.ChunkUploadDtos.CompleteUploadResponse;
import io.github.wickhe4.chunkupload.dto.ChunkUploadDtos.InitUploadRequest;
import io.github.wickhe4.chunkupload.dto.ChunkUploadDtos.InitUploadResponse;
import io.github.wickhe4.chunkupload.dto.ChunkUploadDtos.SelectOption;
import io.github.wickhe4.chunkupload.dto.ChunkUploadDtos.UploadOptionsResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.security.Principal;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.MediaTypeFactory;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import run.halo.app.core.extension.attachment.Attachment;
import run.halo.app.core.extension.attachment.Attachment.AttachmentStatus;
import run.halo.app.core.extension.attachment.Group;
import run.halo.app.core.extension.attachment.Policy;
import run.halo.app.core.extension.service.AttachmentService;
import run.halo.app.extension.AbstractExtension;
import run.halo.app.extension.ReactiveExtensionClient;
import run.halo.app.plugin.PluginContext;

@Slf4j
@Service
public class ChunkUploadService {

    private static final String DEFAULT_POLICY = "default-policy";
    private static final String HIDDEN_LABEL = "halo.run/hidden";
    private static final long MIN_CHUNK_SIZE = 512L * 1024L;          // 512 KB
    private static final long MAX_CHUNK_SIZE = 95L * 1024L * 1024L;   // 95 MB (安全低于 Cloudflare 100MB 限制)
    public static final int MAX_TOTAL_CHUNKS = 10_000;                // 单文件最大分片数上限，防止超大分片数 DoS
    private static final Duration SESSION_EXPIRE_TTL = Duration.ofHours(24);

    private final AttachmentService attachmentService;
    private final ReactiveExtensionClient extensionClient;
    private final Path rootDir;
    private final Set<String> mergingSessions = ConcurrentHashMap.newKeySet();

    @Autowired
    public ChunkUploadService(
        AttachmentService attachmentService,
        ReactiveExtensionClient extensionClient,
        PluginContext pluginContext
    ) {
        this(
            attachmentService,
            extensionClient,
            Path.of(
                System.getProperty("java.io.tmpdir"),
                pluginContext != null && StringUtils.hasText(pluginContext.getName())
                    ? pluginContext.getName()
                    : "plugin-chunk-upload"
            )
        );
    }

    public ChunkUploadService(
        AttachmentService attachmentService,
        ReactiveExtensionClient extensionClient,
        Path rootDir
    ) {
        this.attachmentService = attachmentService;
        this.extensionClient = extensionClient;
        this.rootDir = rootDir;
    }

    /**
     * 响应式查询 Halo 当前可用的存储策略（Policy）与附件分组（Group），排除系统隐藏项（如 UserAvatar）。
     */
    public Mono<UploadOptionsResponse> getUploadOptions() {
        Mono<List<SelectOption>> policiesMono = extensionClient.list(
                Policy.class,
                this::isNotHidden,
                Comparator.comparing(policy -> policy.getMetadata().getCreationTimestamp(),
                    Comparator.nullsLast(Comparator.naturalOrder())))
            .map(policy -> new SelectOption(
                policy.getMetadata().getName(),
                policy.getSpec() != null && StringUtils.hasText(policy.getSpec().getDisplayName())
                    ? policy.getSpec().getDisplayName()
                    : policy.getMetadata().getName()))
            .collectList();

        Mono<List<SelectOption>> groupsMono = extensionClient.list(
                Group.class,
                this::isNotHidden,
                Comparator.comparing(group -> group.getMetadata().getCreationTimestamp(),
                    Comparator.nullsLast(Comparator.naturalOrder())))
            .map(group -> new SelectOption(
                group.getMetadata().getName(),
                group.getSpec() != null && StringUtils.hasText(group.getSpec().getDisplayName())
                    ? group.getSpec().getDisplayName()
                    : group.getMetadata().getName()))
            .collectList();

        return Mono.zip(policiesMono, groupsMono, UploadOptionsResponse::new);
    }

    private boolean isNotHidden(AbstractExtension extension) {
        if (extension == null || extension.getMetadata() == null) {
            return false;
        }
        var labels = extension.getMetadata().getLabels();
        return labels == null || !"true".equalsIgnoreCase(labels.get(HIDDEN_LABEL));
    }

    /**
     * 初始化或恢复分片上传会话（支持刷新页面后通过确定性指纹自动断点续传）。
     */
    public Mono<InitUploadResponse> init(InitUploadRequest request) {
        return Mono.defer(() -> {
            if (request == null || !StringUtils.hasText(request.filename())) {
                return Mono.error(new ResponseStatusException(HttpStatus.BAD_REQUEST, "文件名不能为空"));
            }
            if (request.totalSize() == null || request.totalSize() <= 0) {
                return Mono.error(new ResponseStatusException(HttpStatus.BAD_REQUEST, "文件大小必须大于 0"));
            }
            long chunkSize = request.chunkSize() == null ? 5L * 1024 * 1024 : request.chunkSize();
            if (chunkSize < MIN_CHUNK_SIZE || chunkSize > MAX_CHUNK_SIZE) {
                return Mono.error(new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "分片大小需在 512KB ~ 95MB 之间（以绕过 Cloudflare 100MB 限制）"));
            }
            long rawChunks = (request.totalSize() - 1) / chunkSize + 1;
            if (rawChunks <= 0 || rawChunks > MAX_TOTAL_CHUNKS) {
                return Mono.error(new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "分片总数超过上限（最多支持 " + MAX_TOTAL_CHUNKS + " 片），请调大单次分片大小"));
            }
            int totalChunks = (int) rawChunks;
            String safeName = sanitizeFilename(request.filename());

            return Mono.zip(
                    currentUsername(),
                    resolvePolicyName(request.policyName()),
                    resolveGroupName(request.groupName())
                )
                .flatMap(tuple -> Mono.fromCallable(() -> initSessionOnDisk(
                        request, tuple.getT1(), safeName, tuple.getT2(), tuple.getT3(), chunkSize, totalChunks))
                    .subscribeOn(Schedulers.boundedElastic()));
        });
    }

    private InitUploadResponse initSessionOnDisk(
        InitUploadRequest request,
        String owner,
        String safeName,
        String policyName,
        String groupName,
        long chunkSize,
        int totalChunks
    ) {
        cleanupExpiredSessions();

        String mediaType = StringUtils.hasText(request.mediaType()) ? request.mediaType().trim() : "";
        long lastModified = request.lastModified() == null ? 0L : request.lastModified();

        // 生成绑定当前用户与文件特征的确定性 uploadId，保证同一用户同一文件中断后重传可精准复用已有分片
        String uploadId = buildDeterministicUploadId(
            owner, safeName, request.totalSize(), chunkSize, lastModified, policyName, groupName
        );

        try {
            Path sessionDir = sessionDir(uploadId);
            boolean dirExisted = Files.isDirectory(sessionDir);
            Files.createDirectories(sessionDir);

            SessionMeta meta = new SessionMeta(
                uploadId, owner, safeName, request.totalSize(), chunkSize, totalChunks,
                mediaType, policyName, groupName, Instant.now().toEpochMilli()
            );
            saveMeta(sessionDir, meta);

            Set<Integer> uploadedChunks = scanValidChunks(sessionDir, meta);
            boolean resumed = dirExisted && !uploadedChunks.isEmpty();
            return new InitUploadResponse(uploadId, totalChunks, chunkSize, uploadedChunks, resumed);
        } catch (IOException e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "初始化上传会话失败", e);
        }
    }

    /**
     * 查询指定上传会话的已上传分片状态（校验会话归属）。
     */
    public Mono<InitUploadResponse> status(String uploadId) {
        String safeId = safeUploadId(uploadId);
        return currentUsername()
            .flatMap(owner -> Mono.fromCallable(() -> {
                SessionMeta meta = loadMeta(safeId, owner);
                Set<Integer> uploadedChunks = scanValidChunks(sessionDir(safeId), meta);
                return new InitUploadResponse(
                    safeId, meta.totalChunks(), meta.chunkSize(), uploadedChunks, !uploadedChunks.isEmpty()
                );
            }).subscribeOn(Schedulers.boundedElastic()));
    }

    /**
     * 接收单个分片并校验落盘字节数（先写独立随机临时文件再原子重命名，避免并发重试写冲突）。
     */
    public Mono<ChunkUploadResponse> uploadChunk(String uploadId, int index, FilePart filePart) {
        String safeId = safeUploadId(uploadId);
        return currentUsername()
            .flatMap(owner -> Mono.fromCallable(() -> {
                SessionMeta meta = loadMeta(safeId, owner);
                if (index < 0 || index >= meta.totalChunks()) {
                    throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "分片序号越界: " + index);
                }
                Path sessionDir = sessionDir(safeId);
                Files.createDirectories(sessionDir);
                return meta;
            }).subscribeOn(Schedulers.boundedElastic()))
            .flatMap(meta -> {
                Path sessionDir = sessionDir(safeId);
                Path tempChunk = sessionDir.resolve(index + "." + UUID.randomUUID() + ".part.uploading");
                Path finalChunk = chunkPath(sessionDir, index);
                long expectedSize = expectedChunkSize(meta, index);

                return filePart.transferTo(tempChunk)
                    .then(Mono.fromCallable(() -> {
                        long actualSize = Files.size(tempChunk);
                        if (actualSize != expectedSize) {
                            Files.deleteIfExists(tempChunk);
                            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                                String.format("分片 %d 大小不匹配（期望 %d 字节，实际 %d 字节）", index, expectedSize, actualSize));
                        }
                        moveChunkAtomically(tempChunk, finalChunk);
                        touchSession(sessionDir);
                        return new ChunkUploadResponse(safeId, index, Set.of(index));
                    }).subscribeOn(Schedulers.boundedElastic()))
                    .doFinally(signalType -> {
                        try {
                            Files.deleteIfExists(tempChunk);
                        } catch (IOException ignored) {
                        }
                    });
            });
    }

    /**
     * 合并所有分片 -> 自动识别真实 MediaType -> 调用 Halo AttachmentService.upload() -> 成功后在 boundedElastic 线程清理临时目录。
     */
    public Mono<CompleteUploadResponse> complete(String uploadId) {
        String safeId = safeUploadId(uploadId);
        if (!mergingSessions.add(safeId)) {
            return Mono.error(new ResponseStatusException(HttpStatus.CONFLICT, "当前上传会话正在合并中，请勿重复提交"));
        }
        return currentUsername()
            .flatMap(owner -> Mono.fromCallable(() -> mergeChunksToFile(safeId, owner))
                .subscribeOn(Schedulers.boundedElastic()))
            .flatMap(mergedContext -> {
                var dataBufferFlux = DataBufferUtils.read(
                    mergedContext.mergedFile(),
                    new DefaultDataBufferFactory(),
                    64 * 1024
                );
                String groupName = StringUtils.hasText(mergedContext.meta().groupName())
                    ? mergedContext.meta().groupName()
                    : null;

                return attachmentService.upload(
                        mergedContext.meta().policyName(),
                        groupName,
                        mergedContext.meta().filename(),
                        dataBufferFlux,
                        mergedContext.mediaType()
                    )
                    .flatMap(this::populatePermalink)
                    .map(attachment -> new CompleteUploadResponse(
                        safeId,
                        attachment.getMetadata().getName(),
                        attachment.getSpec().getDisplayName(),
                        attachment.getStatus() != null ? attachment.getStatus().getPermalink() : null,
                        attachment.getSpec().getMediaType(),
                        attachment.getSpec().getSize() != null ? attachment.getSpec().getSize() : mergedContext.meta().totalSize(),
                        attachment
                    ))
                    .flatMap(res -> Mono.fromRunnable(() -> deleteDirectoryQuietly(sessionDir(safeId)))
                        .subscribeOn(Schedulers.boundedElastic())
                        .thenReturn(res));
            })
            .doFinally(signalType -> mergingSessions.remove(safeId));
    }

    /**
     * 取消上传并彻底清理服务端对应的临时分片目录（校验会话归属）。
     */
    public Mono<Void> cancel(String uploadId) {
        String safeId = safeUploadId(uploadId);
        return currentUsername()
            .flatMap(owner -> Mono.fromRunnable(() -> {
                Path dir = sessionDir(safeId);
                if (Files.isDirectory(dir)) {
                    loadMeta(safeId, owner);
                    deleteDirectoryQuietly(dir);
                }
            }).subscribeOn(Schedulers.boundedElastic()))
            .then();
    }

    /**
     * 清理超过 24 小时未活跃的废弃上传会话目录（跳过正在合并入库的会话）。
     */
    public void cleanupExpiredSessions() {
        if (!Files.isDirectory(rootDir)) {
            return;
        }
        long expireBefore = Instant.now().minus(SESSION_EXPIRE_TTL).toEpochMilli();
        try (Stream<Path> dirs = Files.list(rootDir)) {
            dirs.filter(Files::isDirectory).forEach(dir -> {
                String sessionId = dir.getFileName().toString();
                if (mergingSessions.contains(sessionId)) {
                    return;
                }
                try {
                    long lastModified = Files.getLastModifiedTime(dir).toMillis();
                    Path metaFile = dir.resolve("meta.properties");
                    if (Files.isRegularFile(metaFile)) {
                        lastModified = Math.max(lastModified, Files.getLastModifiedTime(metaFile).toMillis());
                    }
                    if (lastModified < expireBefore) {
                        deleteDirectoryQuietly(dir);
                    }
                } catch (IOException ignored) {
                }
            });
        } catch (IOException e) {
            log.debug("清理过期分片会话目录失败: {}", e.getMessage());
        }
    }

    /**
     * 卸载插件时彻底清理所有临时分片目录。
     */
    public void cleanupAllSessions() {
        deleteDirectoryQuietly(rootDir);
    }

    private Mono<String> currentUsername() {
        return ReactiveSecurityContextHolder.getContext()
            .mapNotNull(SecurityContext::getAuthentication)
            .filter(auth -> auth.isAuthenticated() && StringUtils.hasText(auth.getName()))
            .map(Principal::getName)
            .switchIfEmpty(Mono.error(() -> new ResponseStatusException(
                HttpStatus.UNAUTHORIZED, "未获取到当前登录用户身份")));
    }

    private void moveChunkAtomically(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private void touchSession(Path sessionDir) {
        try {
            FileTime now = FileTime.fromMillis(System.currentTimeMillis());
            Files.setLastModifiedTime(sessionDir, now);
            Path metaFile = sessionDir.resolve("meta.properties");
            if (Files.isRegularFile(metaFile)) {
                Files.setLastModifiedTime(metaFile, now);
            }
        } catch (IOException ignored) {
        }
    }

    private MergedContext mergeChunksToFile(String uploadId, String currentUser) throws IOException {
        SessionMeta meta = loadMeta(uploadId, currentUser);
        Path sessionDir = sessionDir(uploadId);
        Path mergedFile = mergedFilePath(sessionDir, meta);

        // 若上次已合并完成但入库失败（如存储策略大小限制拦截），重试时直接复用合并文件
        if (Files.isRegularFile(mergedFile) && Files.size(mergedFile) == meta.totalSize()) {
            MediaType resolvedMediaType = resolveMediaType(meta.mediaType(), meta.filename(), mergedFile);
            return new MergedContext(meta, mergedFile, resolvedMediaType);
        }

        Set<Integer> validChunks = scanValidChunks(sessionDir, meta);
        if (validChunks.size() != meta.totalChunks()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                String.format("分片尚未全部上传完成（已上传 %d / %d）", validChunks.size(), meta.totalChunks()));
        }

        try (FileChannel outChannel = FileChannel.open(
                mergedFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            for (int i = 0; i < meta.totalChunks(); i++) {
                Path chunk = chunkPath(sessionDir, i);
                try (FileChannel inChannel = FileChannel.open(chunk, StandardOpenOption.READ)) {
                    long size = inChannel.size();
                    long position = 0;
                    while (position < size) {
                        long transferred = inChannel.transferTo(position, size - position, outChannel);
                        if (transferred <= 0) {
                            break;
                        }
                        position += transferred;
                    }
                }
                // 合入一片后立即删除该分片，降低磁盘峰值占用
                Files.deleteIfExists(chunk);
            }
        }

        long mergedSize = Files.size(mergedFile);
        if (mergedSize != meta.totalSize()) {
            Files.deleteIfExists(mergedFile);
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                String.format("合并后文件大小（%d）与声明大小（%d）不一致", mergedSize, meta.totalSize()));
        }

        MediaType resolvedMediaType = resolveMediaType(meta.mediaType(), meta.filename(), mergedFile);
        return new MergedContext(meta, mergedFile, resolvedMediaType);
    }

    private Mono<Attachment> populatePermalink(Attachment attachment) {
        return attachmentService.getPermalink(attachment)
            .doOnNext(permalink -> {
                if (attachment.getStatus() == null) {
                    attachment.setStatus(new AttachmentStatus());
                }
                attachment.getStatus().setPermalink(permalink.toString());
            })
            .thenReturn(attachment)
            .onErrorReturn(attachment);
    }

    private MediaType resolveMediaType(String clientMediaType, String filename, Path mergedFile) {
        if (StringUtils.hasText(clientMediaType)) {
            try {
                MediaType parsed = MediaType.parseMediaType(clientMediaType);
                if (!MediaType.APPLICATION_OCTET_STREAM.equals(parsed)) {
                    return parsed;
                }
            } catch (Exception ignored) {
            }
        }
        var byFilename = MediaTypeFactory.getMediaType(filename);
        if (byFilename.isPresent()) {
            return byFilename.get();
        }
        try {
            String probed = Files.probeContentType(mergedFile);
            if (StringUtils.hasText(probed)) {
                return MediaType.parseMediaType(probed);
            }
        } catch (Exception ignored) {
        }
        return MediaType.APPLICATION_OCTET_STREAM;
    }

    private Mono<String> resolvePolicyName(String requestedPolicy) {
        if (StringUtils.hasText(requestedPolicy)) {
            String trimmed = requestedPolicy.trim();
            return extensionClient.fetch(Policy.class, trimmed)
                .filter(this::isNotHidden)
                .map(p -> trimmed)
                .switchIfEmpty(Mono.error(() -> new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "指定的附件存储策略不存在或不可用: " + trimmed)));
        }
        return extensionClient.fetch(Policy.class, DEFAULT_POLICY)
            .filter(this::isNotHidden)
            .map(p -> DEFAULT_POLICY)
            .switchIfEmpty(Mono.defer(() -> extensionClient.list(
                    Policy.class,
                    this::isNotHidden,
                    Comparator.comparing(p -> p.getMetadata().getCreationTimestamp(),
                        Comparator.nullsLast(Comparator.naturalOrder())))
                .next()
                .map(p -> p.getMetadata().getName())
                .switchIfEmpty(Mono.error(() -> new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Halo 尚未配置任何可用的附件存储策略")))));
    }

    private Mono<String> resolveGroupName(String requestedGroup) {
        if (!StringUtils.hasText(requestedGroup)) {
            return Mono.just("");
        }
        String trimmed = requestedGroup.trim();
        return extensionClient.fetch(Group.class, trimmed)
            .filter(this::isNotHidden)
            .map(g -> trimmed)
            .switchIfEmpty(Mono.error(() -> new ResponseStatusException(
                HttpStatus.BAD_REQUEST, "指定的附件分组不存在或不可用: " + trimmed)));
    }

    private Set<Integer> scanValidChunks(Path sessionDir, SessionMeta meta) {
        Set<Integer> valid = new TreeSet<>();
        if (!Files.isDirectory(sessionDir)) {
            return valid;
        }
        Path mergedFile = mergedFilePath(sessionDir, meta);
        try {
            if (Files.isRegularFile(mergedFile)) {
                if (Files.size(mergedFile) == meta.totalSize()) {
                    IntStream.range(0, meta.totalChunks()).forEach(valid::add);
                    return valid;
                }
                Files.deleteIfExists(mergedFile);
            }
        } catch (IOException ignored) {
        }
        try (Stream<Path> files = Files.list(sessionDir)) {
            files.filter(Files::isRegularFile).forEach(part -> {
                String name = part.getFileName().toString();
                if (!name.endsWith(".part")) {
                    return;
                }
                String indexStr = name.substring(0, name.length() - 5);
                try {
                    int index = Integer.parseInt(indexStr);
                    if (index >= 0 && index < meta.totalChunks()) {
                        long expected = expectedChunkSize(meta, index);
                        if (Files.size(part) == expected) {
                            valid.add(index);
                        } else {
                            Files.deleteIfExists(part);
                        }
                    }
                } catch (NumberFormatException | IOException ignored) {
                }
            });
        } catch (IOException ignored) {
        }
        return valid;
    }

    private long expectedChunkSize(SessionMeta meta, int index) {
        if (index < meta.totalChunks() - 1) {
            return meta.chunkSize();
        }
        long remainder = meta.totalSize() - (long) (meta.totalChunks() - 1) * meta.chunkSize();
        return remainder > 0 ? remainder : meta.chunkSize();
    }

    private String buildDeterministicUploadId(
        String owner,
        String filename,
        long totalSize,
        long chunkSize,
        long lastModified,
        String policyName,
        String groupName
    ) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            String raw = String.join("|",
                owner,
                filename,
                String.valueOf(totalSize),
                String.valueOf(chunkSize),
                String.valueOf(lastModified),
                policyName,
                groupName
            );
            byte[] hash = digest.digest(raw.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash).substring(0, 32);
        } catch (Exception e) {
            throw new IllegalStateException("无法生成上传会话 ID", e);
        }
    }

    private void saveMeta(Path sessionDir, SessionMeta meta) throws IOException {
        Properties props = new Properties();
        props.setProperty("uploadId", meta.uploadId());
        props.setProperty("owner", meta.owner());
        props.setProperty("filename", meta.filename());
        props.setProperty("totalSize", String.valueOf(meta.totalSize()));
        props.setProperty("chunkSize", String.valueOf(meta.chunkSize()));
        props.setProperty("totalChunks", String.valueOf(meta.totalChunks()));
        props.setProperty("mediaType", meta.mediaType() == null ? "" : meta.mediaType());
        props.setProperty("policyName", meta.policyName());
        props.setProperty("groupName", meta.groupName() == null ? "" : meta.groupName());
        props.setProperty("updatedAt", String.valueOf(meta.updatedAt()));
        try (OutputStream out = Files.newOutputStream(sessionDir.resolve("meta.properties"))) {
            props.store(out, "Halo Chunk Upload Session");
        }
    }

    private SessionMeta loadMeta(String uploadId, String currentUser) {
        Path metaFile = sessionDir(safeUploadId(uploadId)).resolve("meta.properties");
        if (!Files.isRegularFile(metaFile)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "上传会话不存在或已过期: " + uploadId);
        }
        Properties props = new Properties();
        SessionMeta meta;
        try (InputStream in = Files.newInputStream(metaFile)) {
            props.load(in);
            meta = new SessionMeta(
                props.getProperty("uploadId"),
                props.getProperty("owner", ""),
                props.getProperty("filename"),
                Long.parseLong(props.getProperty("totalSize")),
                Long.parseLong(props.getProperty("chunkSize")),
                Integer.parseInt(props.getProperty("totalChunks")),
                props.getProperty("mediaType", ""),
                props.getProperty("policyName", DEFAULT_POLICY),
                props.getProperty("groupName", ""),
                Long.parseLong(props.getProperty("updatedAt", "0"))
            );
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "读取上传会话元数据失败", e);
        }
        if (!StringUtils.hasText(meta.owner()) || !meta.owner().equals(currentUser)) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "无权操作其他用户的上传会话");
        }
        return meta;
    }

    private Path sessionDir(String uploadId) {
        return rootDir.resolve(safeUploadId(uploadId));
    }

    private Path chunkPath(Path sessionDir, int index) {
        return sessionDir.resolve(index + ".part");
    }

    private Path mergedFilePath(Path sessionDir, SessionMeta meta) {
        return sessionDir.resolve("merged-" + meta.filename());
    }

    private String safeUploadId(String uploadId) {
        if (!StringUtils.hasText(uploadId) || !uploadId.matches("^[a-fA-F0-9]{16,64}$")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "非法的 uploadId");
        }
        return uploadId;
    }

    String sanitizeFilename(String filename) {
        String normalized = filename.replace('\\', '/');
        int lastSlash = normalized.lastIndexOf('/');
        String baseName = lastSlash >= 0 ? normalized.substring(lastSlash + 1) : normalized;
        String cleaned = baseName
            .replaceAll("[\\x00-\\x1F\\x7F\\\\/:*?\"<>|]", "_")
            .trim();
        if (cleaned.isEmpty() || ".".equals(cleaned) || "..".equals(cleaned)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "文件名不合法");
        }
        return cleaned;
    }

    private void deleteDirectoryQuietly(Path dir) {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException ignored) {
                }
            });
        } catch (IOException ignored) {
        }
    }

    private record SessionMeta(
        String uploadId,
        String owner,
        String filename,
        long totalSize,
        long chunkSize,
        int totalChunks,
        String mediaType,
        String policyName,
        String groupName,
        long updatedAt
    ) {
    }

    private record MergedContext(
        SessionMeta meta,
        Path mergedFile,
        MediaType mediaType
    ) {
    }
}
