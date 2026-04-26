package com.datasetviz.service;

import com.datasetviz.config.HdfsProperties;
import com.datasetviz.dto.ImportLocalDirectoryRequest;
import com.datasetviz.model.DatasetRegistration;
import com.datasetviz.model.DatasetType;
import com.datasetviz.model.HdfsFileDescriptor;
import com.datasetviz.util.PathUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

@Service
public class DatasetImportService {

    private final HdfsStorageService hdfsStorageService;
    private final DatasetRegistryService datasetRegistryService;
    private final HdfsProperties hdfsProperties;
    private final DatasetProcessingStateService datasetProcessingStateService;
    private final DatasetAnalyticsService datasetAnalyticsService;

    public DatasetImportService(HdfsStorageService hdfsStorageService,
                                  DatasetRegistryService datasetRegistryService,
                                  HdfsProperties hdfsProperties) {
        this(hdfsStorageService, datasetRegistryService, hdfsProperties, new DatasetProcessingStateService(), null);
    }

    @Autowired
    public DatasetImportService(HdfsStorageService hdfsStorageService,
                                 DatasetRegistryService datasetRegistryService,
                                 HdfsProperties hdfsProperties,
                                 DatasetProcessingStateService datasetProcessingStateService,
                                 DatasetAnalyticsService datasetAnalyticsService) {
        this.hdfsStorageService = hdfsStorageService;
        this.datasetRegistryService = datasetRegistryService;
        this.hdfsProperties = hdfsProperties;
        this.datasetProcessingStateService = datasetProcessingStateService;
        this.datasetAnalyticsService = datasetAnalyticsService;
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
            mirrorFile(file, targetPath);
            hdfsStorageService.copyLocalFileToHdfs(file, targetPath);
        }

        invalidateAnalyticsCache(dataset.getId());

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
            Path mirrorPath = mirrorPath(targetPath);
            if (mirrorPath != null) {
                Files.createDirectories(mirrorPath.getParent());
                try (InputStream inputStream = file.getInputStream()) {
                    Files.copy(inputStream, mirrorPath, StandardCopyOption.REPLACE_EXISTING);
                }
            }
            try (InputStream inputStream = file.getInputStream()) {
                hdfsStorageService.writeToHdfs(inputStream, targetPath);
            }
        }

        invalidateAnalyticsCache(dataset.getId());

        return listDatasetFiles(dataset.getId(), 500);
    }

    public boolean deleteDatasetFile(UUID datasetId, String filePath) throws IOException {
        DatasetRegistration dataset = datasetRegistryService.getRequired(datasetId);
        String targetPath = PathUtils.requireDatasetChildPath(dataset.getHdfsPath(), filePath);
        if (datasetProcessingStateService.isFileLocked(targetPath)) {
            throw new IllegalStateException("File is currently being processed and cannot be deleted: " + targetPath);
        }
        deleteMirrorFile(targetPath);
        boolean deleted = hdfsStorageService.delete(targetPath);
        if (deleted) {
            invalidateAnalyticsCache(dataset.getId());
        }
        return deleted;
    }

    private void invalidateAnalyticsCache(UUID datasetId) {
        if (datasetAnalyticsService != null) {
            datasetAnalyticsService.invalidateCache(datasetId);
        }
    }

    public List<HdfsFileDescriptor> listDatasetFiles(UUID datasetId, int limit) throws IOException {
        DatasetRegistration dataset = datasetRegistryService.getRequired(datasetId);
        if (!hdfsStorageService.exists(dataset.getHdfsPath())) {
            restoreMirror(dataset.getHdfsPath());
        }
        if (!hdfsStorageService.exists(dataset.getHdfsPath())) {
            return List.of();
        }
        List<HdfsFileDescriptor> files = hdfsStorageService.listFiles(dataset.getHdfsPath(), true, Math.max(1, limit));
        if (files.isEmpty()) {
            restoreMirror(dataset.getHdfsPath());
            files = hdfsStorageService.listFiles(dataset.getHdfsPath(), true, Math.max(1, limit));
        }
        return files;
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

    private void mirrorFile(Path sourcePath, String hdfsPath) throws IOException {
        Path mirrorPath = mirrorPath(hdfsPath);
        if (mirrorPath == null) {
            return;
        }
        Files.createDirectories(mirrorPath.getParent());
        Files.copy(sourcePath, mirrorPath, StandardCopyOption.REPLACE_EXISTING);
    }

    private void restoreMirror(String hdfsRootPath) throws IOException {
        Path mirrorRoot = mirrorPath(hdfsRootPath);
        if (mirrorRoot == null || !Files.exists(mirrorRoot)) {
            return;
        }
        hdfsStorageService.createDirectories(hdfsRootPath);
        try (Stream<Path> stream = Files.walk(mirrorRoot)) {
            for (Path file : stream.filter(Files::isRegularFile).toList()) {
                String relativePath = mirrorRoot.relativize(file).toString().replace('\\', '/');
                hdfsStorageService.copyLocalFileToHdfs(file, PathUtils.resolveHdfsPath(hdfsRootPath, relativePath));
            }
        }
    }

    private void deleteMirrorFile(String hdfsPath) throws IOException {
        Path mirrorPath = mirrorPath(hdfsPath);
        if (mirrorPath != null) {
            Files.deleteIfExists(mirrorPath);
        }
    }

    private Path mirrorPath(String hdfsPath) throws IOException {
        if (!hdfsProperties.getEmbedded().isEnabled()) {
            return null;
        }
        java.io.File mirrorDir = hdfsProperties.getEmbedded().getMirrorDir();
        if (mirrorDir == null) {
            mirrorDir = new java.io.File(".hdfs-mirror");
        }
        Path mirrorRoot = mirrorDir.toPath().toAbsolutePath().normalize();
        String normalizedHdfsPath = PathUtils.normalizeHdfsPath(hdfsPath);
        if (normalizedHdfsPath == null || normalizedHdfsPath.isBlank()) {
            return mirrorRoot;
        }
        String relativePath = normalizedHdfsPath.startsWith("/") ? normalizedHdfsPath.substring(1) : normalizedHdfsPath;
        Path mirrorPath = mirrorRoot.resolve(relativePath).normalize();
        if (!mirrorPath.startsWith(mirrorRoot)) {
            throw new IOException("Resolved mirror path escapes mirror directory: " + hdfsPath);
        }
        return mirrorPath;
    }
}
