package com.datasetviz.controller;

import com.datasetviz.dto.ImportLocalDirectoryRequest;
import com.datasetviz.dto.RegisterDatasetRequest;
import com.datasetviz.model.DatasetRegistration;
import com.datasetviz.model.DatasetType;
import com.datasetviz.model.HdfsFileDescriptor;
import com.datasetviz.service.DatasetImportService;
import com.datasetviz.service.DatasetAnalyticsService;
import com.datasetviz.service.DatasetRegistryService;
import com.datasetviz.service.HdfsStorageService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/datasets")
public class DatasetController {

    private final DatasetRegistryService datasetRegistryService;
    private final DatasetImportService datasetImportService;
    private final DatasetAnalyticsService datasetAnalyticsService;
    private final HdfsStorageService hdfsStorageService;

    public DatasetController(DatasetRegistryService datasetRegistryService,
                             DatasetImportService datasetImportService,
                             DatasetAnalyticsService datasetAnalyticsService,
                             HdfsStorageService hdfsStorageService) {
        this.datasetRegistryService = datasetRegistryService;
        this.datasetImportService = datasetImportService;
        this.datasetAnalyticsService = datasetAnalyticsService;
        this.hdfsStorageService = hdfsStorageService;
    }

    @PostMapping("/register")
    public ResponseEntity<DatasetRegistration> register(@Valid @RequestBody RegisterDatasetRequest request) throws IOException {
        if (!hdfsStorageService.exists(request.getHdfsPath())) {
            hdfsStorageService.createDirectories(request.getHdfsPath());
        }
        DatasetRegistration registration = datasetRegistryService.register(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(registration);
    }

    @PostMapping("/import-local")
    public ResponseEntity<List<HdfsFileDescriptor>> importLocal(@Valid @RequestBody ImportLocalDirectoryRequest request) throws IOException {
        return ResponseEntity.status(HttpStatus.CREATED).body(datasetImportService.importLocalDirectory(request));
    }

    @PostMapping("/{datasetId}/import-remote")
    public ResponseEntity<List<HdfsFileDescriptor>> importRemote(@PathVariable UUID datasetId,
                                                                 @RequestParam(defaultValue = "GENERIC_FILES") DatasetType datasetType,
                                                                 @RequestParam("files") MultipartFile[] files,
                                                                 @RequestParam(required = false) String targetSubdirectory) throws IOException {
        return ResponseEntity.status(HttpStatus.CREATED).body(datasetImportService.importRemoteFiles(datasetId, datasetType, files, targetSubdirectory));
    }

    @PostMapping("/{datasetId}/dashboard/cancel")
    public ResponseEntity<Void> cancelDashboard(@PathVariable UUID datasetId) {
        boolean cancelled = datasetAnalyticsService.cancel(datasetId);
        return cancelled ? ResponseEntity.accepted().build() : ResponseEntity.noContent().build();
    }

    @DeleteMapping("/{datasetId}/files")
    public ResponseEntity<Void> deleteFile(@PathVariable UUID datasetId,
                                           @RequestParam String path) throws IOException {
        boolean deleted = datasetImportService.deleteDatasetFile(datasetId, path);
        return deleted ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
    }

    @GetMapping
    public List<DatasetRegistration> listDatasets() {
        return datasetRegistryService.listAll();
    }

    @GetMapping("/{datasetId}")
    public DatasetRegistration getDataset(@PathVariable UUID datasetId) {
        return datasetRegistryService.getRequired(datasetId);
    }

    @GetMapping("/{datasetId}/files")
    public List<HdfsFileDescriptor> listFiles(@PathVariable UUID datasetId,
                                                @RequestParam(defaultValue = "50") int limit,
                                                @RequestParam(defaultValue = "true") boolean recursive) throws IOException {
        if (recursive) {
            return datasetImportService.listDatasetFiles(datasetId, Math.max(1, limit));
        }
        DatasetRegistration dataset = datasetRegistryService.getRequired(datasetId);
        if (!hdfsStorageService.exists(dataset.getHdfsPath())) {
            datasetImportService.listDatasetFiles(datasetId, Math.max(1, limit));
        }
        if (!hdfsStorageService.exists(dataset.getHdfsPath())) {
            return List.of();
        }
        return hdfsStorageService.listFiles(dataset.getHdfsPath(), false, Math.max(1, limit));
    }
}
