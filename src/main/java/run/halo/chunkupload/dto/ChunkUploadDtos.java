package run.halo.chunkupload.dto;

import java.util.List;
import java.util.Set;
import run.halo.app.core.extension.attachment.Attachment;

public final class ChunkUploadDtos {

    private ChunkUploadDtos() {
    }

    public record SelectOption(String name, String displayName) {
    }

    public record UploadOptionsResponse(
        List<SelectOption> policies,
        List<SelectOption> groups
    ) {
    }

    public record InitUploadRequest(
        String filename,
        Long totalSize,
        Long chunkSize,
        Long lastModified,
        String mediaType,
        String policyName,
        String groupName
    ) {
    }

    public record InitUploadResponse(
        String uploadId,
        int totalChunks,
        long chunkSize,
        Set<Integer> uploadedChunks,
        boolean resumed
    ) {
    }

    public record ChunkUploadResponse(
        String uploadId,
        int chunkIndex,
        Set<Integer> uploadedChunks
    ) {
    }

    public record CompleteUploadResponse(
        String uploadId,
        String attachmentName,
        String displayName,
        String permalink,
        String mediaType,
        long size,
        Attachment attachment
    ) {
    }
}
