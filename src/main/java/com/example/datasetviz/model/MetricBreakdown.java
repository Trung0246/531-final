package com.example.datasetviz.model;

import java.util.ArrayList;
import java.util.List;

public class MetricBreakdown {

    private String name;
    private List<NamedCount> items = new ArrayList<>();

    public MetricBreakdown() {
    }

    public MetricBreakdown(String name, List<NamedCount> items) {
        this.name = name;
        this.items = items;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<NamedCount> getItems() {
        return items;
    }

    public void setItems(List<NamedCount> items) {
        this.items = items;
    }
}
