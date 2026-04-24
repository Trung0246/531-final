package com.datasetviz.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.io.File;

@ConfigurationProperties(prefix = "app.registry")
public class DatasetRegistryProperties {

    private File path = new File("dataset-registry.json");

    public File getPath() {
        return path;
    }

    public void setPath(File path) {
        this.path = path;
    }
}
