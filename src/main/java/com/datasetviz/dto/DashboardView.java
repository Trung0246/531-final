package com.datasetviz.dto;

import com.datasetviz.model.DatasetType;

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
        List<ColumnPreview> columnProfiles,
        ListPanel listPanel,
        TablePanel tablePanel
) {

    public record SummaryItem(String label, String value) {
    }

    public record Chart(String id, String title, String type, List<Series> series, List<String> availableModes, String semanticType) {

        public Chart(String id, String title, String type, List<Series> series) {
            this(id, title, type, series, List.of(type, "TABLE"), null);
        }
    }

    public record Series(String name, List<Point> points) {
    }

    public record Point(String label, double value) {
    }

    public record ColumnPreview(String name,
                                String type,
                                List<String> sampleValues,
                                int blankCount,
                                int nonBlankCount,
                                int distinctCount,
                                List<Point> topValues,
                                List<Point> histogramBuckets) {
    }

    public record ListPanel(String title, List<String> items) {
    }

    public record TablePanel(String title, List<String> columns, List<TableRow> rows) {
    }

    public record TableRow(List<String> cells) {
    }
}
