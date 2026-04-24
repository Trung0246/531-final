package com.datasetviz.model;

import java.time.Instant;
import java.util.UUID;

public class DatasetRegistration {

    private UUID id;
    private String name;
    private String description;
    private DatasetType datasetType;
    private String hdfsPath;
    private Instant registeredAt;

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
}
