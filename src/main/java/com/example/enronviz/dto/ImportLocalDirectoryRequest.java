package com.example.enronviz.dto;

import com.example.enronviz.model.DatasetType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public class ImportLocalDirectoryRequest {

    @NotBlank
    private String name;

    private String description;

    @NotNull
    private DatasetType datasetType = DatasetType.ENRON_EMAIL;

    @NotBlank
    private String localDirectory;

    @NotBlank
    private String targetHdfsPath;

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

    public String getLocalDirectory() {
        return localDirectory;
    }

    public void setLocalDirectory(String localDirectory) {
        this.localDirectory = localDirectory;
    }

    public String getTargetHdfsPath() {
        return targetHdfsPath;
    }

    public void setTargetHdfsPath(String targetHdfsPath) {
        this.targetHdfsPath = targetHdfsPath;
    }
}
