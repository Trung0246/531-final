package com.example.datasetviz.dto;

import com.example.datasetviz.model.DatasetType;

import java.util.List;

public record DashboardView(
        String datasetId,
        String datasetName,
        DatasetType datasetType,
        String hdfsPath,
        String generatedAt,
        int maxFiles,
        List<SummaryItem> summaryItems,
        List<Chart> charts,
        ListPanel listPanel,
        TablePanel tablePanel
) {

    public record SummaryItem(String label, String value) {
    }

    public record Chart(String id, String title, String type, List<Series> series) {
    }

    public record Series(String name, List<Point> points) {
    }

    public record Point(String label, double value) {
    }

    public record ListPanel(String title, List<String> items) {
    }

    public record TablePanel(String title, List<String> columns, List<TableRow> rows) {
    }

    public record TableRow(List<String> cells) {
    }
}
