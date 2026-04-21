package com.example.datasetviz.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class EmailAnalyticsSnapshot {

    private UUID datasetId;
    private String datasetName;
    private DatasetType datasetType;
    private String hdfsPath;
    private Instant generatedAt;
    private int maxFiles;
    private AnalyticsOverview overview;
    private List<TimeSeriesPoint> volumeByMonth = new ArrayList<>();
    private List<TimeSeriesPoint> hourlyDistribution = new ArrayList<>();
    private List<NamedCount> topSenders = new ArrayList<>();
    private List<NamedCount> topRecipients = new ArrayList<>();
    private List<NamedCount> topMailboxOwners = new ArrayList<>();
    private List<NamedCount> topSubjectKeywords = new ArrayList<>();
    private List<CommunicationEdge> communicationGraph = new ArrayList<>();

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

    public AnalyticsOverview getOverview() {
        return overview;
    }

    public void setOverview(AnalyticsOverview overview) {
        this.overview = overview;
    }

    public List<TimeSeriesPoint> getVolumeByMonth() {
        return volumeByMonth;
    }

    public void setVolumeByMonth(List<TimeSeriesPoint> volumeByMonth) {
        this.volumeByMonth = volumeByMonth;
    }

    public List<TimeSeriesPoint> getHourlyDistribution() {
        return hourlyDistribution;
    }

    public void setHourlyDistribution(List<TimeSeriesPoint> hourlyDistribution) {
        this.hourlyDistribution = hourlyDistribution;
    }

    public List<NamedCount> getTopSenders() {
        return topSenders;
    }

    public void setTopSenders(List<NamedCount> topSenders) {
        this.topSenders = topSenders;
    }

    public List<NamedCount> getTopRecipients() {
        return topRecipients;
    }

    public void setTopRecipients(List<NamedCount> topRecipients) {
        this.topRecipients = topRecipients;
    }

    public List<NamedCount> getTopMailboxOwners() {
        return topMailboxOwners;
    }

    public void setTopMailboxOwners(List<NamedCount> topMailboxOwners) {
        this.topMailboxOwners = topMailboxOwners;
    }

    public List<NamedCount> getTopSubjectKeywords() {
        return topSubjectKeywords;
    }

    public void setTopSubjectKeywords(List<NamedCount> topSubjectKeywords) {
        this.topSubjectKeywords = topSubjectKeywords;
    }

    public List<CommunicationEdge> getCommunicationGraph() {
        return communicationGraph;
    }

    public void setCommunicationGraph(List<CommunicationEdge> communicationGraph) {
        this.communicationGraph = communicationGraph;
    }
}
