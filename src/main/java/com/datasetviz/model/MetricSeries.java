package com.datasetviz.model;

import java.util.ArrayList;
import java.util.List;

public class MetricSeries {

    private String name;
    private List<TimeSeriesPoint> points = new ArrayList<>();

    public MetricSeries() {
    }

    public MetricSeries(String name, List<TimeSeriesPoint> points) {
        this.name = name;
        this.points = points;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<TimeSeriesPoint> getPoints() {
        return points;
    }

    public void setPoints(List<TimeSeriesPoint> points) {
        this.points = points;
    }
}
