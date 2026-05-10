package com.datasetviz.dto;

import jakarta.validation.constraints.NotBlank;

public class RegisterDatasetRequest {

    @NotBlank
    private String name;

    private String description;

    @NotBlank
    private String hdfsPath;

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

    public String getHdfsPath() {
        return hdfsPath;
    }

    public void setHdfsPath(String hdfsPath) {
        this.hdfsPath = hdfsPath;
    }
}
