package com.datasetviz.controller;

import com.datasetviz.dto.ImportLocalDirectoryRequest;
import com.datasetviz.dto.RegisterDatasetRequest;
import com.datasetviz.model.DatasetRegistration;
import com.datasetviz.model.DatasetType;
import com.datasetviz.model.HdfsFileDescriptor;
import com.datasetviz.service.ImportService;
import com.datasetviz.service.AnalyticsService;
import com.datasetviz.service.RegistryService;
import com.datasetviz.service.HdfsStore;
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
public class DatasetApi {

    private final RegistryService registry;
    private final ImportService imports;
    private final AnalyticsService analytics;
    private final HdfsStore hdfs;

    public DatasetApi(RegistryService registry,
                             ImportService imports,
                             AnalyticsService analytics,
                             HdfsStore hdfs) {
        this.registry = registry;
        this.imports = imports;
        this.analytics = analytics;
        this.hdfs = hdfs;
    }

    @PostMapping("/register")
    public ResponseEntity<DatasetRegistration> register(@Valid @RequestBody RegisterDatasetRequest request) throws IOException {
        if (!hdfs.exists(request.getHdfsPath())) {
            hdfs.createDirectories(request.getHdfsPath());
        }
        DatasetRegistration registration = registry.register(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(registration);
    }

    @PostMapping("/import-local")
    public ResponseEntity<List<HdfsFileDescriptor>> importLocal(@Valid @RequestBody ImportLocalDirectoryRequest request) throws IOException {
        return ResponseEntity.status(HttpStatus.CREATED).body(imports.importLocalDirectory(request));
    }

    @PostMapping("/{datasetId}/import-remote")
    public ResponseEntity<List<HdfsFileDescriptor>> importRemote(@PathVariable UUID datasetId,
                                                                 @RequestParam(defaultValue = "GENERIC_FILES") DatasetType datasetType,
                                                                 @RequestParam("files") MultipartFile[] files,
                                                                 @RequestParam(required = false) String targetSubdirectory) throws IOException {
        return ResponseEntity.status(HttpStatus.CREATED).body(imports.importRemoteFiles(datasetId, datasetType, files, targetSubdirectory));
    }

    @PostMapping("/{datasetId}/dashboard/cancel")
    public ResponseEntity<Void> cancelDashboard(@PathVariable UUID datasetId) {
        boolean done = analytics.cancel(datasetId);
        return done ? ResponseEntity.accepted().build() : ResponseEntity.noContent().build();
    }

    @DeleteMapping("/{datasetId}/files")
    public ResponseEntity<Void> deleteFile(@PathVariable UUID datasetId,
                                            @RequestParam String path) throws IOException {
        boolean deleted = imports.deleteDatasetFile(datasetId, path);
        return deleted ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
    }

    @DeleteMapping("/{datasetId}")
    public ResponseEntity<Void> deleteDataset(@PathVariable UUID datasetId) throws IOException {
        imports.deleteDataset(datasetId);
        return ResponseEntity.noContent().build();
    }

    @GetMapping
    public List<DatasetRegistration> listDatasets() {
        return registry.listAll();
    }

    @GetMapping("/{datasetId}")
    public DatasetRegistration getDataset(@PathVariable UUID datasetId) {
        return registry.getRequired(datasetId);
    }

    @GetMapping("/{datasetId}/files")
    public List<HdfsFileDescriptor> listFiles(@PathVariable UUID datasetId,
                                                @RequestParam(defaultValue = "50") int limit,
                                                @RequestParam(defaultValue = "true") boolean recursive) throws IOException {
        if (recursive) {
            return imports.listDatasetFiles(datasetId, Math.max(1, limit));
        }
        DatasetRegistration dataset = registry.getRequired(datasetId);
        if (!hdfs.exists(dataset.getHdfsPath())) {
            imports.listDatasetFiles(datasetId, Math.max(1, limit));
        }
        if (!hdfs.exists(dataset.getHdfsPath())) {
            return List.of();
        }
        return hdfs.listFiles(dataset.getHdfsPath(), false, Math.max(1, limit));
    }
}
