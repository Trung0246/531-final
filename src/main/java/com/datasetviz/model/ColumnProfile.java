package com.datasetviz.model;

import java.util.List;

public class ColumnProfile {

    private String name;
    private String type;
    private List<String> sampleValues;

    public ColumnProfile() {
    }

    public ColumnProfile(String name, String type, List<String> sampleValues) {
        this.name = name;
        this.type = type;
        this.sampleValues = sampleValues;
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
}
