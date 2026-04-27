package com.datasetviz.model;

import java.util.List;

public class ColumnProfile {

    private String name;
    private String type;
    private List<String> sampleValues;
    private int blankCount;
    private int nonBlankCount;
    private int distinctCount;
    private List<NamedCount> topValues;
    private List<NamedCount> histogramBuckets;

    public ColumnProfile() {
    }

    public ColumnProfile(String name, String type, List<String> sampleValues) {
        this(name, type, sampleValues, 0, sampleValues == null ? 0 : sampleValues.size(), sampleValues == null ? 0 : sampleValues.size(), List.of(), List.of());
    }

    public ColumnProfile(String name,
                         String type,
                         List<String> sampleValues,
                         int blankCount,
                         int nonBlankCount,
                         int distinctCount,
                         List<NamedCount> topValues,
                         List<NamedCount> histogramBuckets) {
        this.name = name;
        this.type = type;
        this.sampleValues = sampleValues == null ? List.of() : sampleValues;
        this.blankCount = blankCount;
        this.nonBlankCount = nonBlankCount;
        this.distinctCount = distinctCount;
        this.topValues = topValues == null ? List.of() : topValues;
        this.histogramBuckets = histogramBuckets == null ? List.of() : histogramBuckets;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<String> getSampleValues() {
        return sampleValues;
    }

    public void setSampleValues(List<String> sampleValues) {
        this.sampleValues = sampleValues;
    }

    public int getBlankCount() {
        return blankCount;
    }

    public void setBlankCount(int blankCount) {
        this.blankCount = blankCount;
    }

    public int getNonBlankCount() {
        return nonBlankCount;
    }

    public void setNonBlankCount(int nonBlankCount) {
        this.nonBlankCount = nonBlankCount;
    }

    public int getDistinctCount() {
        return distinctCount;
    }

    public void setDistinctCount(int distinctCount) {
        this.distinctCount = distinctCount;
    }

    public List<NamedCount> getTopValues() {
        return topValues;
    }

    public void setTopValues(List<NamedCount> topValues) {
        this.topValues = topValues;
    }

    public List<NamedCount> getHistogramBuckets() {
        return histogramBuckets;
    }

    public void setHistogramBuckets(List<NamedCount> histogramBuckets) {
        this.histogramBuckets = histogramBuckets;
    }
}
