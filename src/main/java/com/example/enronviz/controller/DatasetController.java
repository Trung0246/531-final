package com.example.enronviz.controller;

import com.example.enronviz.dto.ImportLocalDirectoryRequest;
import com.example.enronviz.dto.RegisterDatasetRequest;
import com.example.enronviz.model.DatasetRegistration;
import com.example.enronviz.model.HdfsFileDescriptor;
import com.example.enronviz.service.DatasetImportService;
import com.example.enronviz.service.DatasetRegistryService;
import com.example.enronviz.service.HdfsStorageService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/datasets")
public class DatasetController {

    private final DatasetRegistryService datasetRegistryService;
    private final DatasetImportService datasetImportService;
    private final HdfsStorageService hdfsStorageService;

    public DatasetController(DatasetRegistryService datasetRegistryService,
                             DatasetImportService datasetImportService,
                             HdfsStorageService hdfsStorageService) {
        this.datasetRegistryService = datasetRegistryService;
        this.datasetImportService = datasetImportService;
        this.hdfsStorageService = hdfsStorageService;
    }

    @PostMapping("/register")
    public ResponseEntity<DatasetRegistration> register(@Valid @RequestBody RegisterDatasetRequest request) throws IOException {
        if (!hdfsStorageService.exists(request.getHdfsPath())) {
            throw new IllegalArgumentException("HDFS path does not exist: " + request.getHdfsPath());
        }
        DatasetRegistration registration = datasetRegistryService.register(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(registration);
    }

    @PostMapping("/import-local")
    public ResponseEntity<DatasetRegistration> importLocal(@Valid @RequestBody ImportLocalDirectoryRequest request) throws IOException {
        DatasetRegistration registration = datasetImportService.importLocalDirectory(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(registration);
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
        DatasetRegistration dataset = datasetRegistryService.getRequired(datasetId);
        return hdfsStorageService.listFiles(dataset.getHdfsPath(), recursive, Math.max(1, limit));
    }
}
