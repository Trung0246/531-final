package com.datasetviz.dto;

import com.datasetviz.model.DatasetType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.UUID;

public class ImportLocalDirectoryRequest {

    @NotNull
    private UUID datasetId;

    @NotNull
    private DatasetType datasetType = DatasetType.GENERIC_FILES;

    @NotBlank
    private String localDirectory;

    private String targetSubdirectory;

    public UUID getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(UUID datasetId) {
        this.datasetId = datasetId;
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

    public String getTargetSubdirectory() {
        return targetSubdirectory;
    }

    public void setTargetSubdirectory(String targetSubdirectory) {
        this.targetSubdirectory = targetSubdirectory;
    }
}
