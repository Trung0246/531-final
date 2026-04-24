package com.example.datasetviz.service;

import com.example.datasetviz.config.HdfsProperties;
import com.example.datasetviz.dto.ImportLocalDirectoryRequest;
import com.example.datasetviz.dto.RegisterDatasetRequest;
import com.example.datasetviz.model.DatasetRegistration;
import com.example.datasetviz.util.PathUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

@Service
public class DatasetImportService {

    private final HdfsStorageService hdfsStorageService;
    private final DatasetRegistryService datasetRegistryService;
    private final HdfsProperties hdfsProperties;

    public DatasetImportService(HdfsStorageService hdfsStorageService,
                                DatasetRegistryService datasetRegistryService,
                                HdfsProperties hdfsProperties) {
        this.hdfsStorageService = hdfsStorageService;
        this.datasetRegistryService = datasetRegistryService;
        this.hdfsProperties = hdfsProperties;
    }

    public DatasetRegistration importLocalDirectory(ImportLocalDirectoryRequest request) throws IOException {
        Path localDirectory = Paths.get(request.getLocalDirectory()).normalize();
        if (!Files.exists(localDirectory) || !Files.isDirectory(localDirectory)) {
            throw new IllegalArgumentException("Local directory does not exist: " + localDirectory);
        }
        localDirectory = localDirectory.toRealPath();
        validateLocalDirectoryRoot(localDirectory);

        String targetHdfsPath = resolveTargetHdfsPath(request.getTargetHdfsPath());

        hdfsStorageService.createDirectories(targetHdfsPath);

        List<Path> localFiles;
        try (Stream<Path> stream = Files.walk(localDirectory)) {
            localFiles = stream.filter(Files::isRegularFile).toList();
        }

        for (Path file : localFiles) {
            String relativePath = localDirectory.relativize(file).toString().replace('\\', '/');
            String targetPath = PathUtils.resolveHdfsPath(targetHdfsPath, relativePath);
            hdfsStorageService.copyLocalFileToHdfs(file, targetPath);
        }

        RegisterDatasetRequest registerDatasetRequest = new RegisterDatasetRequest();
        registerDatasetRequest.setName(request.getName());
        registerDatasetRequest.setDescription(request.getDescription());
        registerDatasetRequest.setDatasetType(request.getDatasetType());
        registerDatasetRequest.setHdfsPath(targetHdfsPath);
        return datasetRegistryService.register(registerDatasetRequest);
    }

    private String resolveTargetHdfsPath(String requestedPath) {
        String normalizedPath = PathUtils.normalizeHdfsPath(requestedPath);
        if (!StringUtils.hasText(hdfsProperties.getHdfsPath())) {
            return normalizedPath;
        }

        String normalizedBasePath = PathUtils.normalizeHdfsPath(hdfsProperties.getHdfsPath());
        String relativePath = normalizedPath;
        if (relativePath.startsWith("/")) {
            relativePath = relativePath.substring(1);
        }
        return PathUtils.resolveHdfsPath(normalizedBasePath, relativePath);
    }

    private void validateLocalDirectoryRoot(Path localDirectory) throws IOException {
        if (!StringUtils.hasText(hdfsProperties.getLocalPath())) {
            return;
        }

        Path allowedRoot = Paths.get(hdfsProperties.getLocalPath()).normalize();
        if (!Files.exists(allowedRoot) || !Files.isDirectory(allowedRoot)) {
            throw new IllegalArgumentException("Configured local path does not exist: " + allowedRoot);
        }

        Path allowedRootRealPath = allowedRoot.toRealPath();
        if (!localDirectory.startsWith(allowedRootRealPath)) {
            throw new IllegalArgumentException("Local directory must be under configured local path: " + allowedRootRealPath);
        }
    }
}
