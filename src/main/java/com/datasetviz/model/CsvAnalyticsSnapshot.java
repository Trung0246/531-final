package com.datasetviz.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class CsvAnalyticsSnapshot {

    private UUID datasetId;
    private String datasetName;
    private DatasetType datasetType;
    private String hdfsPath;
    private Instant generatedAt;
    private int maxFiles;
    private CsvAnalyticsOverview overview;
    private List<TimeSeriesPoint> rowsByDate = new ArrayList<>();
    private List<MetricSeries> metricTimeSeries = new ArrayList<>();
    private List<NamedCount> metricTotals = new ArrayList<>();
    private List<MetricBreakdown> topLocationsByMetric = new ArrayList<>();

    public UUID getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(UUID datasetId) {
        this.datasetId = datasetId;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
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

    public Instant getGeneratedAt() {
        return generatedAt;
    }

    public void setGeneratedAt(Instant generatedAt) {
        this.generatedAt = generatedAt;
    }

    public int getMaxFiles() {
        return maxFiles;
    }

    public void setMaxFiles(int maxFiles) {
        this.maxFiles = maxFiles;
    }

    public CsvAnalyticsOverview getOverview() {
        return overview;
    }

    public void setOverview(CsvAnalyticsOverview overview) {
        this.overview = overview;
    }

    public List<TimeSeriesPoint> getRowsByDate() {
        return rowsByDate;
    }

    public void setRowsByDate(List<TimeSeriesPoint> rowsByDate) {
        this.rowsByDate = rowsByDate;
    }

    public List<MetricSeries> getMetricTimeSeries() {
        return metricTimeSeries;
    }

    public void setMetricTimeSeries(List<MetricSeries> metricTimeSeries) {
        this.metricTimeSeries = metricTimeSeries;
    }

    public List<NamedCount> getMetricTotals() {
        return metricTotals;
    }

    public void setMetricTotals(List<NamedCount> metricTotals) {
        this.metricTotals = metricTotals;
    }

    public List<MetricBreakdown> getTopLocationsByMetric() {
        return topLocationsByMetric;
    }

    public void setTopLocationsByMetric(List<MetricBreakdown> topLocationsByMetric) {
        this.topLocationsByMetric = topLocationsByMetric;
    }
}
