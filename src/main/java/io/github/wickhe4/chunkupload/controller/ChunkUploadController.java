package io.github.wickhe4.chunkupload.controller;

import io.github.wickhe4.chunkupload.dto.ChunkUploadDtos.ChunkUploadResponse;
import io.github.wickhe4.chunkupload.dto.ChunkUploadDtos.CompleteUploadResponse;
import io.github.wickhe4.chunkupload.dto.ChunkUploadDtos.InitUploadRequest;
import io.github.wickhe4.chunkupload.dto.ChunkUploadDtos.InitUploadResponse;
import io.github.wickhe4.chunkupload.dto.ChunkUploadDtos.UploadOptionsResponse;
import io.github.wickhe4.chunkupload.service.ChunkUploadService;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import run.halo.app.plugin.ApiVersion;

@ApiVersion("console.api.chunkupload.halo.run/v1alpha1")
@RestController
@RequestMapping("/uploads")
public class ChunkUploadController {

    private final ChunkUploadService chunkUploadService;

    public ChunkUploadController(ChunkUploadService chunkUploadService) {
        this.chunkUploadService = chunkUploadService;
    }

    @GetMapping("/options")
    public Mono<UploadOptionsResponse> options() {
        return chunkUploadService.getUploadOptions();
    }

    @PostMapping("/-/init")
    public Mono<InitUploadResponse> init(@RequestBody InitUploadRequest request) {
        return chunkUploadService.init(request);
    }

    @GetMapping("/{uploadId}")
    public Mono<InitUploadResponse> status(@PathVariable("uploadId") String uploadId) {
        return chunkUploadService.status(uploadId);
    }

    @PostMapping(value = "/{uploadId}/chunks/{index}", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Mono<ChunkUploadResponse> uploadChunk(
        @PathVariable("uploadId") String uploadId,
        @PathVariable("index") int index,
        @RequestPart("file") FilePart filePart
    ) {
        return chunkUploadService.uploadChunk(uploadId, index, filePart);
    }

    @PostMapping("/{uploadId}/complete")
    public Mono<CompleteUploadResponse> complete(@PathVariable("uploadId") String uploadId) {
        return chunkUploadService.complete(uploadId);
    }

    @DeleteMapping("/{uploadId}")
    public Mono<Void> cancel(@PathVariable("uploadId") String uploadId) {
        return chunkUploadService.cancel(uploadId);
    }
}
