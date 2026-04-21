package com.example.datasetviz.service;

import com.example.datasetviz.dto.ImportLocalDirectoryRequest;
import com.example.datasetviz.dto.RegisterDatasetRequest;
import com.example.datasetviz.model.DatasetRegistration;
import com.example.datasetviz.util.PathUtils;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

@Service
public class DatasetImportService {

    private final HdfsStorageService hdfsStorageService;
    private final DatasetRegistryService datasetRegistryService;

    public DatasetImportService(HdfsStorageService hdfsStorageService,
                                DatasetRegistryService datasetRegistryService) {
        this.hdfsStorageService = hdfsStorageService;
        this.datasetRegistryService = datasetRegistryService;
    }

    public DatasetRegistration importLocalDirectory(ImportLocalDirectoryRequest request) throws IOException {
        java.nio.file.Path localDirectory = Paths.get(request.getLocalDirectory()).normalize();
        if (!Files.exists(localDirectory) || !Files.isDirectory(localDirectory)) {
            throw new IllegalArgumentException("Local directory does not exist: " + localDirectory);
        }

        hdfsStorageService.createDirectories(request.getTargetHdfsPath());

        List<java.nio.file.Path> localFiles;
        try (Stream<java.nio.file.Path> stream = Files.walk(localDirectory)) {
            localFiles = stream.filter(Files::isRegularFile).toList();
        }

        for (java.nio.file.Path file : localFiles) {
            String relativePath = localDirectory.relativize(file).toString().replace('\\', '/');
            String targetPath = PathUtils.resolveHdfsPath(request.getTargetHdfsPath(), relativePath);
            hdfsStorageService.copyLocalFileToHdfs(file, targetPath);
        }

        RegisterDatasetRequest registerDatasetRequest = new RegisterDatasetRequest();
        registerDatasetRequest.setName(request.getName());
        registerDatasetRequest.setDescription(request.getDescription());
        registerDatasetRequest.setDatasetType(request.getDatasetType());
        registerDatasetRequest.setHdfsPath(request.getTargetHdfsPath());
        return datasetRegistryService.register(registerDatasetRequest);
    }
}
