package com.example.enronviz.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "app.analytics")
public class AnalyticsProperties {

    private int defaultMaxFiles = 5000;
    private int maxFilesHardLimit = 20000;
    private Duration cacheTtl = Duration.ofMinutes(10);
    private int defaultTopLimit = 10;
    private int defaultGraphEdgeLimit = 50;

    public int getDefaultMaxFiles() {
        return defaultMaxFiles;
    }

    public void setDefaultMaxFiles(int defaultMaxFiles) {
        this.defaultMaxFiles = defaultMaxFiles;
    }

    public int getMaxFilesHardLimit() {
        return maxFilesHardLimit;
    }

    public void setMaxFilesHardLimit(int maxFilesHardLimit) {
        this.maxFilesHardLimit = maxFilesHardLimit;
    }

    public Duration getCacheTtl() {
        return cacheTtl;
    }

    public void setCacheTtl(Duration cacheTtl) {
        this.cacheTtl = cacheTtl;
    }

    public int getDefaultTopLimit() {
        return defaultTopLimit;
    }

    public void setDefaultTopLimit(int defaultTopLimit) {
        this.defaultTopLimit = defaultTopLimit;
    }

    public int getDefaultGraphEdgeLimit() {
        return defaultGraphEdgeLimit;
    }

    public void setDefaultGraphEdgeLimit(int defaultGraphEdgeLimit) {
        this.defaultGraphEdgeLimit = defaultGraphEdgeLimit;
    }
}
