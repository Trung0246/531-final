package com.datasetviz.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class DatasetRegistration {

    private UUID id;
    private String name;
    private String description;
    private DatasetType datasetType;
    private String hdfsPath;
    private Instant registeredAt;
    private List<PendingLocalImport> pendingLocalImports = new ArrayList<>();

    public DatasetRegistration() {
    }

    public DatasetRegistration(UUID id,
                               String name,
                               String description,
                               DatasetType datasetType,
                               String hdfsPath,
                               Instant registeredAt) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.datasetType = datasetType;
        this.hdfsPath = hdfsPath;
        this.registeredAt = registeredAt;
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public DatasetType getDatasetType() {
        return datasetType;
    }

    public void setDatasetType(DatasetType datasetType) {
        this.datasetType = datasetType;
    }

    public String getHdfsPath() {
        return hdfsPath;
    }

    public void setHdfsPath(String hdfsPath) {
        this.hdfsPath = hdfsPath;
    }

    public Instant getRegisteredAt() {
        return registeredAt;
    }

    public void setRegisteredAt(Instant registeredAt) {
        this.registeredAt = registeredAt;
    }

    public List<PendingLocalImport> getPendingLocalImports() {
        if (pendingLocalImports == null) {
            pendingLocalImports = new ArrayList<>();
        }
        return pendingLocalImports;
    }

    public void setPendingLocalImports(List<PendingLocalImport> pendingLocalImports) {
        this.pendingLocalImports = pendingLocalImports == null ? new ArrayList<>() : new ArrayList<>(pendingLocalImports);
    }

    public void addPendingLocalImport(String localPath, String targetSubdirectory) {
        if (localPath == null || localPath.isBlank()) {
            return;
        }
        boolean exists = getPendingLocalImports().stream().anyMatch(importEntry ->
                Objects.equals(localPath, importEntry.getLocalPath())
                        && Objects.equals(targetSubdirectory, importEntry.getTargetSubdirectory()));
        if (!exists) {
            pendingLocalImports.add(new PendingLocalImport(localPath, targetSubdirectory));
        }
    }

    public void clearPendingLocalImports() {
        getPendingLocalImports().clear();
    }

    public static class PendingLocalImport {
        private String localPath;
        private String targetSubdirectory;

        public PendingLocalImport() {
        }

        public PendingLocalImport(String localPath, String targetSubdirectory) {
            this.localPath = localPath;
            this.targetSubdirectory = targetSubdirectory;
        }

        public String getLocalPath() {
            return localPath;
        }

        public void setLocalPath(String localPath) {
            this.localPath = localPath;
        }

        public String getTargetSubdirectory() {
            return targetSubdirectory;
        }

        public void setTargetSubdirectory(String targetSubdirectory) {
            this.targetSubdirectory = targetSubdirectory;
        }
    }
}
