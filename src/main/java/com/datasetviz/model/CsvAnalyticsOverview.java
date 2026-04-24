package com.datasetviz.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class CsvAnalyticsOverview {

    private int scannedFiles;
    private int processedRows;
    private int failedFiles;
    private int distinctLocations;
    private int detectedMetrics;
    private String dateColumn;
    private String locationColumn;
    private List<String> metricColumns = new ArrayList<>();
    private Instant firstObservedAt;
    private Instant lastObservedAt;

    public int getScannedFiles() {
        return scannedFiles;
    }

    public void setScannedFiles(int scannedFiles) {
        this.scannedFiles = scannedFiles;
    }

    public int getProcessedRows() {
        return processedRows;
    }

    public void setProcessedRows(int processedRows) {
        this.processedRows = processedRows;
    }

    public int getFailedFiles() {
        return failedFiles;
    }

    public void setFailedFiles(int failedFiles) {
        this.failedFiles = failedFiles;
    }

    public int getDistinctLocations() {
        return distinctLocations;
    }

    public void setDistinctLocations(int distinctLocations) {
        this.distinctLocations = distinctLocations;
    }

    public int getDetectedMetrics() {
        return detectedMetrics;
    }

    public void setDetectedMetrics(int detectedMetrics) {
        this.detectedMetrics = detectedMetrics;
    }

    public String getDateColumn() {
        return dateColumn;
    }

    public void setDateColumn(String dateColumn) {
        this.dateColumn = dateColumn;
    }

    public String getLocationColumn() {
        return locationColumn;
    }

    public void setLocationColumn(String locationColumn) {
        this.locationColumn = locationColumn;
    }

    public List<String> getMetricColumns() {
        return metricColumns;
    }

    public void setMetricColumns(List<String> metricColumns) {
        this.metricColumns = metricColumns;
    }

    public Instant getFirstObservedAt() {
        return firstObservedAt;
    }

    public void setFirstObservedAt(Instant firstObservedAt) {
        this.firstObservedAt = firstObservedAt;
    }

    public Instant getLastObservedAt() {
        return lastObservedAt;
    }

    public void setLastObservedAt(Instant lastObservedAt) {
        this.lastObservedAt = lastObservedAt;
    }
}
