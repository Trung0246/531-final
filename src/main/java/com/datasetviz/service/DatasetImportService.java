package com.datasetviz.service;

import com.datasetviz.config.HdfsProperties;
import com.datasetviz.dto.ImportLocalDirectoryRequest;
import com.datasetviz.model.DatasetRegistration;
import com.datasetviz.model.DatasetType;
import com.datasetviz.model.HdfsFileDescriptor;
import com.datasetviz.util.PathUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
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

    public List<HdfsFileDescriptor> importLocalDirectory(ImportLocalDirectoryRequest request) throws IOException {
        DatasetRegistration dataset = datasetRegistryService.updateDatasetType(request.getDatasetId(), request.getDatasetType());
        Path localDirectory = Paths.get(request.getLocalDirectory()).normalize();
        if (!Files.exists(localDirectory) || !Files.isDirectory(localDirectory)) {
            throw new IllegalArgumentException("Local directory does not exist: " + localDirectory);
        }
        localDirectory = localDirectory.toRealPath();
        validateLocalDirectoryRoot(localDirectory);

        String targetHdfsPath = resolveDatasetTargetPath(dataset, request.getTargetSubdirectory());

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

        return listDatasetFiles(dataset.getId(), 500);
    }

    public List<HdfsFileDescriptor> importRemoteFiles(UUID datasetId, DatasetType datasetType, MultipartFile[] files, String targetSubdirectory) throws IOException {
        DatasetRegistration dataset = datasetRegistryService.updateDatasetType(datasetId, datasetType);
        if (files == null || files.length == 0) {
            throw new IllegalArgumentException("At least one file is required");
        }

        String targetHdfsPath = resolveDatasetTargetPath(dataset, targetSubdirectory);
        hdfsStorageService.createDirectories(targetHdfsPath);

        for (MultipartFile file : files) {
            if (file == null || file.isEmpty()) {
                continue;
            }
            String originalFilename = StringUtils.cleanPath(file.getOriginalFilename() == null ? file.getName() : file.getOriginalFilename());
            if (!StringUtils.hasText(originalFilename)) {
                throw new IllegalArgumentException("Uploaded file name is required");
            }
            String targetPath = PathUtils.resolveHdfsPath(targetHdfsPath, originalFilename);
            try (InputStream inputStream = file.getInputStream()) {
                hdfsStorageService.writeToHdfs(inputStream, targetPath);
            }
        }

        return listDatasetFiles(dataset.getId(), 500);
    }

    public boolean deleteDatasetFile(UUID datasetId, String filePath) throws IOException {
        DatasetRegistration dataset = datasetRegistryService.getRequired(datasetId);
        String targetPath = PathUtils.requireDatasetChildPath(dataset.getHdfsPath(), filePath);
        return hdfsStorageService.delete(targetPath);
    }

    public List<HdfsFileDescriptor> listDatasetFiles(UUID datasetId, int limit) throws IOException {
        DatasetRegistration dataset = datasetRegistryService.getRequired(datasetId);
        return hdfsStorageService.listFiles(dataset.getHdfsPath(), true, Math.max(1, limit));
    }

    private String resolveDatasetTargetPath(DatasetRegistration dataset, String targetSubdirectory) {
        return PathUtils.resolveDatasetFilePath(dataset.getHdfsPath(), targetSubdirectory);
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
