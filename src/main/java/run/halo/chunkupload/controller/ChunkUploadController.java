package run.halo.chunkupload.controller;

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
import reactor.core.scheduler.Schedulers;
import run.halo.app.plugin.ApiVersion;
import run.halo.chunkupload.dto.ChunkUploadDtos.ChunkUploadResponse;
import run.halo.chunkupload.dto.ChunkUploadDtos.CompleteUploadResponse;
import run.halo.chunkupload.dto.ChunkUploadDtos.InitUploadRequest;
import run.halo.chunkupload.dto.ChunkUploadDtos.InitUploadResponse;
import run.halo.chunkupload.dto.ChunkUploadDtos.UploadOptionsResponse;
import run.halo.chunkupload.service.ChunkUploadService;

@ApiVersion("chunkupload.halo.run/v1alpha1")
@RestController
@RequestMapping("/uploads")
public class ChunkUploadController {

    private final ChunkUploadService chunkUploadService;

    public ChunkUploadController(ChunkUploadService chunkUploadService) {
        this.chunkUploadService = chunkUploadService;
    }

    @GetMapping("/options")
    public Mono<UploadOptionsResponse> options() {
        return Mono.fromCallable(chunkUploadService::getUploadOptions)
            .subscribeOn(Schedulers.boundedElastic());
    }

    @PostMapping("/init")
    public Mono<InitUploadResponse> init(@RequestBody InitUploadRequest request) {
        return Mono.fromCallable(() -> chunkUploadService.init(request))
            .subscribeOn(Schedulers.boundedElastic());
    }

    @GetMapping("/{uploadId}")
    public Mono<InitUploadResponse> status(@PathVariable("uploadId") String uploadId) {
        return Mono.fromCallable(() -> chunkUploadService.status(uploadId))
            .subscribeOn(Schedulers.boundedElastic());
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
        return Mono.fromRunnable(() -> chunkUploadService.cancel(uploadId))
            .subscribeOn(Schedulers.boundedElastic())
            .then();
    }
}
