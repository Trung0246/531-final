package com.datasetviz.service;

import com.datasetviz.config.HdfsProps;
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
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

@Service
public class ImportService {

    private final HdfsStore hdfsStore;
    private final RegistryService registryService;
    private final HdfsProps hdfsProps;
    private final StateService stateService;
    private final AnalyticsService analyticsService;

    public ImportService(HdfsStore hdfsStore,
                                  RegistryService registryService,
                                  HdfsProps hdfsProps) {
        this(hdfsStore, registryService, hdfsProps, new StateService(), null);
    }

    @Autowired
    public ImportService(HdfsStore hdfsStore,
                                 RegistryService registryService,
                                 HdfsProps hdfsProps,
                                 StateService stateService,
                                 AnalyticsService analyticsService) {
        this.hdfsStore = hdfsStore;
        this.registryService = registryService;
        this.hdfsProps = hdfsProps;
        this.stateService = stateService;
        this.analyticsService = analyticsService;
    }

    public List<HdfsFileDescriptor> importLocalDirectory(ImportLocalDirectoryRequest request) throws IOException {
        DatasetRegistration dataset = registryService.updateDatasetType(request.getDatasetId(), request.getDatasetType());
        Path localDir = Paths.get(request.getLocalDirectory()).normalize();
        if (!Files.exists(localDir) || !Files.isDirectory(localDir)) {
            throw new IllegalArgumentException("Local directory does not exist: " + localDir);
        }
        localDir = localDir.toRealPath();
        checkLocalRoot(localDir);

        String targetRoot = targetPath(dataset, request.getTargetSubdirectory());

        hdfsStore.createDirectories(targetRoot);

        if (dataset.getDatasetType() == DatasetType.EMAIL_ARCHIVE) {
            registryService.updatePendingLocalImport(dataset.getId(), localDir.toString(), request.getTargetSubdirectory());
            clearAnalytics(dataset.getId());
            return listDatasetFiles(dataset.getId(), 500);
        }

        List<Path> files;
        try (Stream<Path> stream = Files.walk(localDir)) {
            files = stream.filter(Files::isRegularFile).toList();
        }

        for (Path file : files) {
            String rel = localDir.relativize(file).toString().replace('\\', '/');
            String targetPath = PathUtils.resolveHdfsPath(targetRoot, rel);
            mirrorFile(file, targetPath);
            hdfsStore.copyLocalFileToHdfs(file, targetPath);
        }

        clearAnalytics(dataset.getId());

        return listDatasetFiles(dataset.getId(), 500);
    }

    public List<HdfsFileDescriptor> importRemoteFiles(UUID datasetId, DatasetType datasetType, MultipartFile[] files, String targetSubdirectory) throws IOException {
        DatasetRegistration dataset = registryService.updateDatasetType(datasetId, datasetType);
        if (files == null || files.length == 0) {
            throw new IllegalArgumentException("At least one file is required");
        }

        String targetRoot = targetPath(dataset, targetSubdirectory);
        hdfsStore.createDirectories(targetRoot);

        for (MultipartFile file : files) {
            if (file == null || file.isEmpty()) {
                continue;
            }
            String name = StringUtils.cleanPath(file.getOriginalFilename() == null ? file.getName() : file.getOriginalFilename());
            if (!StringUtils.hasText(name)) {
                throw new IllegalArgumentException("Uploaded file name is required");
            }
            String targetPath = PathUtils.resolveHdfsPath(targetRoot, name);
            Path mirrorPath = mirrorPath(targetPath);
            if (mirrorPath != null) {
                Files.createDirectories(mirrorPath.getParent());
                try (InputStream inputStream = file.getInputStream()) {
                    Files.copy(inputStream, mirrorPath, StandardCopyOption.REPLACE_EXISTING);
                }
            }
            try (InputStream inputStream = file.getInputStream()) {
                hdfsStore.writeToHdfs(inputStream, targetPath);
            }
        }

        clearAnalytics(dataset.getId());

        return listDatasetFiles(dataset.getId(), 500);
    }

    public boolean deleteDatasetFile(UUID datasetId, String filePath) throws IOException {
        DatasetRegistration dataset = registryService.getRequired(datasetId);
        String targetPath = PathUtils.requireDatasetChildPath(dataset.getHdfsPath(), filePath);
        if (stateService.locked(targetPath)) {
            throw new IllegalStateException("File is currently being processed and cannot be deleted: " + targetPath);
        }
        deleteMirrorFile(targetPath);
        boolean deleted = hdfsStore.delete(targetPath);
        if (deleted) {
            clearAnalytics(dataset.getId());
        }
        return deleted;
    }

    public void deleteDataset(UUID datasetId) throws IOException {
        DatasetRegistration dataset = registryService.getRequired(datasetId);
        String root = PathUtils.normalizeHdfsPath(dataset.getHdfsPath());
        if (root == null || root.isBlank() || root.equals("/")) {
            throw new IllegalArgumentException("Refusing to delete dataset HDFS root: " + root);
        }

        stateService.cancel(datasetId);
        hdfsStore.delete(root);
        deleteMirrorTree(root);
        clearAnalytics(datasetId);
        registryService.deleteDataset(datasetId);
    }

    private void clearAnalytics(UUID datasetId) {
        if (analyticsService != null) {
            analyticsService.invalidateCache(datasetId);
        }
    }


    public List<HdfsFileDescriptor> listDatasetFiles(UUID datasetId, int limit) throws IOException {
        DatasetRegistration dataset = registryService.getRequired(datasetId);
        if (!hdfsStore.exists(dataset.getHdfsPath())) {
            restoreMirror(dataset.getHdfsPath());
        }
        if (!hdfsStore.exists(dataset.getHdfsPath())) {
            return List.of();
        }
        List<HdfsFileDescriptor> files = hdfsStore.listFiles(dataset.getHdfsPath(), true, Math.max(1, limit));
        if (files.isEmpty()) {
            restoreMirror(dataset.getHdfsPath());
            files = hdfsStore.listFiles(dataset.getHdfsPath(), true, Math.max(1, limit));
        }
        return files;
    }

    private String targetPath(DatasetRegistration dataset, String targetSubdirectory) {
        return PathUtils.resolveDatasetFilePath(dataset.getHdfsPath(), targetSubdirectory);
    }

    private void checkLocalRoot(Path localDir) throws IOException {
        if (!StringUtils.hasText(hdfsProps.getLocalPath())) {
            return;
        }

        Path root = Paths.get(hdfsProps.getLocalPath()).normalize();
        if (!Files.exists(root) || !Files.isDirectory(root)) {
            throw new IllegalArgumentException("Configured local path does not exist: " + root);
        }

        Path realRoot = root.toRealPath();
        if (!localDir.startsWith(realRoot)) {
            throw new IllegalArgumentException("Local directory must be under configured local path: " + realRoot);
        }
    }

    private void mirrorFile(Path src, String hdfsPath) throws IOException {
        Path mirrorPath = mirrorPath(hdfsPath);
        if (mirrorPath == null) {
            return;
        }
        Files.createDirectories(mirrorPath.getParent());
        Files.copy(src, mirrorPath, StandardCopyOption.REPLACE_EXISTING);
    }

    private void restoreMirror(String root) throws IOException {
        Path mirrorRoot = mirrorPath(root);
        if (mirrorRoot == null || !Files.exists(mirrorRoot)) {
            return;
        }
        hdfsStore.createDirectories(root);
        try (Stream<Path> stream = Files.walk(mirrorRoot)) {
            for (Path file : stream.filter(Files::isRegularFile).toList()) {
                String rel = mirrorRoot.relativize(file).toString().replace('\\', '/');
                hdfsStore.copyLocalFileToHdfs(file, PathUtils.resolveHdfsPath(root, rel));
            }
        }
    }

    private void deleteMirrorFile(String hdfsPath) throws IOException {
        Path mirrorPath = mirrorPath(hdfsPath);
        if (mirrorPath != null) {
            Files.deleteIfExists(mirrorPath);
        }
    }

    private void deleteMirrorTree(String hdfsPath) throws IOException {
        Path mirrorPath = mirrorPath(hdfsPath);
        if (mirrorPath == null || !Files.exists(mirrorPath)) {
            return;
        }
        try (Stream<Path> stream = Files.walk(mirrorPath)) {
            for (Path path : stream.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }

    private Path mirrorPath(String hdfsPath) throws IOException {
        if (!hdfsProps.getEmbedded().isEnabled()) {
            return null;
        }
        java.io.File mirrorDir = hdfsProps.getEmbedded().getMirrorDir();
        if (mirrorDir == null) {
            mirrorDir = new java.io.File(".hdfs-mirror");
        }
        Path mirrorRoot = mirrorDir.toPath().toAbsolutePath().normalize();
        String norm = PathUtils.normalizeHdfsPath(hdfsPath);
        if (norm == null || norm.isBlank()) {
            return mirrorRoot;
        }
        String rel = norm.startsWith("/") ? norm.substring(1) : norm;
        Path mirrorPath = mirrorRoot.resolve(rel).normalize();
        if (!mirrorPath.startsWith(mirrorRoot)) {
            throw new IOException("Resolved mirror path escapes mirror directory: " + hdfsPath);
        }
        return mirrorPath;
    }
}
