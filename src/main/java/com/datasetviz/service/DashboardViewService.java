package com.datasetviz.service;

import com.datasetviz.dto.DashboardView;
import com.datasetviz.dto.DatasetView;
import com.datasetviz.model.ColumnProfile;
import com.datasetviz.model.CommunicationEdge;
import com.datasetviz.model.CsvAnalyticsOverview;
import com.datasetviz.model.CsvAnalyticsSnapshot;
import com.datasetviz.model.DatasetRegistration;
import com.datasetviz.model.EmailAnalyticsSnapshot;
import com.datasetviz.model.MetricBreakdown;
import com.datasetviz.model.MetricSeries;
import com.datasetviz.model.NamedCount;
import com.datasetviz.model.TimeSeriesPoint;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class DashboardViewService {

    public DashboardView toDashboardView(Object snapshot) {
        if (snapshot instanceof EmailAnalyticsSnapshot emailSnapshot) {
            return fromEmailSnapshot(emailSnapshot);
        }
        if (snapshot instanceof CsvAnalyticsSnapshot csvSnapshot) {
            return fromCsvSnapshot(csvSnapshot);
        }
        throw new IllegalArgumentException("Dashboard view supports EMAIL_ARCHIVE and CSV_TEXT analytics snapshots only.");
    }

    public List<DashboardView.Chart> toDashboardCharts(Object snapshot) {
        if (snapshot instanceof EmailAnalyticsSnapshot emailSnapshot) {
            return emailCharts(emailSnapshot);
        }
        if (snapshot instanceof CsvAnalyticsSnapshot csvSnapshot) {
            return csvCharts(csvSnapshot);
        }
        throw new IllegalArgumentException("Dashboard charts support EMAIL_ARCHIVE and CSV_TEXT analytics snapshots only.");
    }

    public DatasetView toDatasetView(DatasetRegistration registration) {
        return toDatasetView(registration, null);
    }

    public DatasetView toDatasetView(DatasetRegistration registration, Boolean hdfsPathAlreadyExisted) {
        return new DatasetView(
                registration.getId().toString(),
                registration.getName(),
                registration.getDescription(),
                registration.getDatasetType(),
                registration.getHdfsPath(),
                formatInstant(registration.getRegisteredAt()),
                hdfsPathAlreadyExisted
        );
    }

    private DashboardView fromEmailSnapshot(EmailAnalyticsSnapshot snapshot) {
        List<DashboardView.SummaryItem> summaryItems = List.of(
                summaryItem("Dataset", snapshot.getDatasetName()),
                summaryItem("Dataset type", snapshot.getDatasetType().name()),
                summaryItem("Scanned files", snapshot.getOverview().getScannedFiles()),
                summaryItem("Parsed emails", snapshot.getOverview().getParsedEmails()),
                summaryItem("Failed files", snapshot.getOverview().getFailedFiles()),
                summaryItem("Unique senders", snapshot.getOverview().getUniqueSenders()),
                summaryItem("Unique recipients", snapshot.getOverview().getUniqueRecipients()),
                summaryItem("First email", formatInstant(snapshot.getOverview().getFirstEmailAt())),
                summaryItem("Last email", formatInstant(snapshot.getOverview().getLastEmailAt()))
        );

        List<DashboardView.Chart> charts = emailCharts(snapshot);

        return new DashboardView(
                snapshot.getDatasetId().toString(),
                snapshot.getDatasetName(),
                snapshot.getDatasetType(),
                snapshot.getHdfsPath(),
                formatInstant(snapshot.getGeneratedAt()),
                snapshot.getMaxFiles(),
                summaryItems,
                charts,
                List.of(),
                new DashboardView.ListPanel("Top subject keywords", snapshot.getTopSubjectKeywords().stream()
                        .map(item -> item.getName() + " (" + item.getCount() + ")")
                        .toList()),
                new DashboardView.TablePanel(
                        "Communication graph edges",
                        List.of("Source", "Target", "Count"),
                        snapshot.getCommunicationGraph().stream().map(this::toTableRow).toList()
                )
        );
    }

    private List<DashboardView.Chart> emailCharts(EmailAnalyticsSnapshot snapshot) {
        return List.of(
                chart("volume-by-month", "Email volume by month", "BAR", List.of(seriesFromBuckets("Count", snapshot.getVolumeByMonth())), List.of("BAR", "LINE", "TABLE"), "DATE"),
                chart("top-senders", "Top senders", "BAR", List.of(seriesFromPoints("Count", toPoints(snapshot.getTopSenders()))), List.of("BAR", "DONUT", "TABLE"), "CATEGORICAL"),
                chart("top-recipients", "Top recipients", "BAR", List.of(seriesFromPoints("Count", toPoints(snapshot.getTopRecipients()))), List.of("BAR", "DONUT", "TABLE"), "CATEGORICAL"),
                chart("hourly-distribution", "Hourly distribution (UTC)", "LINE", List.of(seriesFromBuckets("Count", snapshot.getHourlyDistribution())), List.of("LINE", "BAR", "TABLE"), "DATE")
        );
    }

    private DashboardView fromCsvSnapshot(CsvAnalyticsSnapshot snapshot) {
        CsvAnalyticsOverview overview = snapshot.getOverview();
        List<MetricBreakdown> topLocationBreakdowns = snapshot.getTopLocationsByMetric() == null ? List.of() : snapshot.getTopLocationsByMetric();

        List<DashboardView.SummaryItem> summaryItems = List.of(
                summaryItem("Dataset", snapshot.getDatasetName()),
                summaryItem("Dataset type", snapshot.getDatasetType().name()),
                summaryItem("Scanned files", overview.getScannedFiles()),
                summaryItem("Processed rows", overview.getProcessedRows()),
                summaryItem("Failed files", overview.getFailedFiles()),
                summaryItem("Detected metrics", overview.getDetectedMetrics()),
                summaryItem("Distinct locations", overview.getDistinctLocations()),
                summaryItem("Date column", defaultValue(overview.getDateColumn())),
                summaryItem("Location column", defaultValue(overview.getLocationColumn())),
                summaryItem("First observation", formatInstant(overview.getFirstObservedAt())),
                summaryItem("Last observation", formatInstant(overview.getLastObservedAt()))
        );

        List<DashboardView.Chart> charts = csvCharts(snapshot);

        return new DashboardView(
                snapshot.getDatasetId().toString(),
                snapshot.getDatasetName(),
                snapshot.getDatasetType(),
                snapshot.getHdfsPath(),
                formatInstant(snapshot.getGeneratedAt()),
                snapshot.getMaxFiles(),
                summaryItems,
                charts,
                toColumnPreviews(snapshot.getColumnProfiles()),
                new DashboardView.ListPanel(
                        "Detected schema and totals",
                        buildCsvListItems(overview, snapshot.getMetricTotals())
                ),
                buildCsvTable(topLocationBreakdowns)
        );
    }

    private List<DashboardView.Chart> csvCharts(CsvAnalyticsSnapshot snapshot) {
        List<MetricBreakdown> topLocationBreakdowns = snapshot.getTopLocationsByMetric() == null ? List.of() : snapshot.getTopLocationsByMetric();
        List<NamedCount> primaryBreakdown = topLocationBreakdowns.isEmpty() ? List.of() : topLocationBreakdowns.get(0).getItems();
        List<MetricSeries> metricSeries = snapshot.getMetricTimeSeries() == null ? List.of() : snapshot.getMetricTimeSeries().stream().limit(3).toList();

        List<DashboardView.Chart> charts = new ArrayList<>();
        charts.add(chart("rows-by-date", "Rows by observation date", "LINE", List.of(seriesFromBuckets("Rows", snapshot.getRowsByDate())), List.of("LINE", "BAR", "TABLE"), "DATE"));
        charts.add(chart("metric-totals", "Latest metric totals", "BAR", List.of(seriesFromPoints("Count", toPoints(snapshot.getMetricTotals()))), List.of("BAR", "DONUT", "TABLE"), "NUMERIC"));
        charts.add(chart(
                        "top-locations",
                        topLocationBreakdowns.isEmpty() ? "Top locations" : "Top locations by " + topLocationBreakdowns.get(0).getName(),
                        "BAR",
                        List.of(seriesFromPoints("Count", toPoints(primaryBreakdown))),
                        List.of("BAR", "DONUT", "TABLE"),
                        "CATEGORICAL"
                ));
        charts.add(chart("metric-trends", "Metric trends over time", "LINE", metricSeries.stream().map(this::series).toList(), List.of("LINE", "TABLE"), "DATE"));
        charts.addAll(columnCharts(snapshot.getColumnProfiles()));
        return charts;
    }

    private List<DashboardView.ColumnPreview> toColumnPreviews(List<ColumnProfile> columnProfiles) {
        return columnProfiles == null ? List.of() : columnProfiles.stream()
                .map(profile -> new DashboardView.ColumnPreview(
                        profile.getName(),
                        profile.getType(),
                        profile.getSampleValues(),
                        profile.getBlankCount(),
                        profile.getNonBlankCount(),
                        profile.getDistinctCount(),
                        toPoints(profile.getTopValues()),
                        toPoints(profile.getHistogramBuckets())
                ))
                .toList();
    }

    private List<DashboardView.Chart> columnCharts(List<ColumnProfile> columnProfiles) {
        if (columnProfiles == null || columnProfiles.isEmpty()) {
            return List.of();
        }

        List<DashboardView.Chart> charts = new ArrayList<>();
        charts.add(chart(
                "column-completeness",
                "Column completeness",
                "MISSINGNESS",
                List.of(
                        new DashboardView.Series("Non-blank", columnProfiles.stream().map(profile -> new DashboardView.Point(profile.getName(), profile.getNonBlankCount())).toList()),
                        new DashboardView.Series("Blank", columnProfiles.stream().map(profile -> new DashboardView.Point(profile.getName(), profile.getBlankCount())).toList())
                ),
                List.of("MISSINGNESS", "BAR", "TABLE"),
                "MISSINGNESS"
        ));

        columnProfiles.stream()
                .filter(profile -> "NUMBER".equals(profile.getType()) && profile.getHistogramBuckets() != null && !profile.getHistogramBuckets().isEmpty())
                .limit(4)
                .map(profile -> chart(
                        "column-" + slug(profile.getName()) + "-distribution",
                        "Distribution: " + profile.getName(),
                        "HISTOGRAM",
                        List.of(seriesFromPoints("Rows", toPoints(profile.getHistogramBuckets()))),
                        List.of("HISTOGRAM", "BAR", "TABLE"),
                        "NUMERIC"
                ))
                .forEach(charts::add);

        columnProfiles.stream()
                .filter(profile -> !"NUMBER".equals(profile.getType()) && profile.getTopValues() != null && !profile.getTopValues().isEmpty())
                .limit(4)
                .map(profile -> chart(
                        "column-" + slug(profile.getName()) + "-top-values",
                        "Top values: " + profile.getName(),
                        "BAR",
                        List.of(seriesFromPoints("Rows", toPoints(profile.getTopValues()))),
                        List.of("BAR", "DONUT", "TABLE"),
                        profile.getType()
                ))
                .forEach(charts::add);

        return charts;
    }

    private List<String> buildCsvListItems(CsvAnalyticsOverview overview, List<NamedCount> metricTotals) {
        List<String> items = new ArrayList<>();
        overview.getMetricColumns().forEach(column -> items.add("Metric: " + column));
        metricTotals.forEach(item -> items.add(item.getName() + ": " + item.getCount()));
        return items;
    }

    private DashboardView.TablePanel buildCsvTable(List<MetricBreakdown> topLocationBreakdowns) {
        List<String> metricNames = topLocationBreakdowns.stream()
                .map(MetricBreakdown::getName)
                .limit(2)
                .toList();
        Map<String, Map<String, Long>> locations = new java.util.LinkedHashMap<>();
        topLocationBreakdowns.stream().limit(2).forEach(breakdown ->
                breakdown.getItems().forEach(item ->
                        locations.computeIfAbsent(item.getName(), ignored -> new java.util.LinkedHashMap<>())
                                .put(breakdown.getName(), item.getCount())));

        List<DashboardView.TableRow> rows = locations.entrySet().stream()
                .map(entry -> new DashboardView.TableRow(List.of(
                        entry.getKey(),
                        String.valueOf(entry.getValue().getOrDefault(metricNames.isEmpty() ? "" : metricNames.get(0), 0L)),
                        String.valueOf(entry.getValue().getOrDefault(metricNames.size() > 1 ? metricNames.get(1) : "", 0L))
                )))
                .sorted((left, right) -> Double.compare(parseDouble(right.cells().get(1)), parseDouble(left.cells().get(1))))
                .toList();

        if (rows.isEmpty()) {
            rows = List.of(new DashboardView.TableRow(List.of("No location data", "—", "—")));
        }

        return new DashboardView.TablePanel(
                "Top locations by metric",
                List.of("Location", metricNames.isEmpty() ? "Metric" : metricNames.get(0), metricNames.size() > 1 ? metricNames.get(1) : "Value"),
                rows
        );
    }

    private DashboardView.TableRow toTableRow(CommunicationEdge edge) {
        return new DashboardView.TableRow(List.of(edge.getSource(), edge.getTarget(), String.valueOf(edge.getCount())));
    }

    private DashboardView.Chart chart(String id, String title, String type, List<DashboardView.Series> series) {
        return chart(id, title, type, series, List.of(type, "TABLE"), null);
    }

    private DashboardView.Chart chart(String id, String title, String type, List<DashboardView.Series> series, List<String> availableModes, String semanticType) {
        return new DashboardView.Chart(id, title, type, series, availableModes, semanticType);
    }

    private DashboardView.Series seriesFromBuckets(String name, List<TimeSeriesPoint> points) {
        return new DashboardView.Series(name, points == null ? List.of() : points.stream()
                .map(point -> new DashboardView.Point(point.getBucket(), point.getCount()))
                .toList());
    }

    private DashboardView.Series seriesFromPoints(String name, List<DashboardView.Point> points) {
        return new DashboardView.Series(name, points == null ? List.of() : points);
    }

    private DashboardView.Series series(MetricSeries series) {
        return new DashboardView.Series(series.getName(), series.getPoints().stream()
                .map(point -> new DashboardView.Point(point.getBucket(), point.getCount()))
                .toList());
    }

    private List<DashboardView.Point> toPoints(List<NamedCount> items) {
        return items == null ? List.of() : items.stream()
                .map(item -> new DashboardView.Point(item.getName(), item.getCount()))
                .toList();
    }

    private String slug(String value) {
        String slug = value == null ? "column" : value.toLowerCase().replaceAll("[^a-z0-9]+", "-").replaceAll("(^-|-$)", "");
        return slug.isBlank() ? "column" : slug;
    }

    private DashboardView.SummaryItem summaryItem(String label, Object value) {
        return new DashboardView.SummaryItem(label, value == null ? "—" : String.valueOf(value));
    }

    private String formatInstant(Instant value) {
        return value == null ? "—" : value.toString();
    }

    private String defaultValue(String value) {
        return value == null || value.isBlank() ? "—" : value;
    }

    private double parseDouble(String value) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException exception) {
            return Double.NEGATIVE_INFINITY;
        }
    }
}
