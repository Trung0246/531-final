package com.example.datasetviz.service;

import com.example.datasetviz.config.AnalyticsProperties;
import com.example.datasetviz.model.CsvAnalyticsOverview;
import com.example.datasetviz.model.CsvAnalyticsSnapshot;
import com.example.datasetviz.model.DatasetRegistration;
import com.example.datasetviz.model.DatasetType;
import com.example.datasetviz.model.MetricBreakdown;
import com.example.datasetviz.model.MetricSeries;
import com.example.datasetviz.model.NamedCount;
import com.example.datasetviz.model.TimeSeriesPoint;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.springframework.stereotype.Service;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Service
public class CsvAnalyticsService {

    private static final List<String> DATE_COLUMN_CANDIDATES = List.of(
            "observationdate", "date", "reportdate", "lastupdate", "lastupdated", "updatedate"
    );

    private static final List<String> LOCATION_COLUMN_CANDIDATES = List.of(
            "countryregion", "country_region", "country", "location", "region", "provincestate", "province_state", "state", "province"
    );

    private static final List<String> PREFERRED_METRIC_CANDIDATES = List.of(
            "confirmed", "deaths", "recovered", "active", "cases"
    );

    private static final List<DateTimeFormatter> DATE_FORMATTERS = List.of(
            DateTimeFormatter.ISO_OFFSET_DATE_TIME,
            DateTimeFormatter.ISO_ZONED_DATE_TIME,
            DateTimeFormatter.ISO_LOCAL_DATE_TIME,
            DateTimeFormatter.ISO_LOCAL_DATE,
            DateTimeFormatter.ofPattern("M/d/yyyy H:mm", Locale.ENGLISH),
            DateTimeFormatter.ofPattern("M/d/yyyy H:mm:ss", Locale.ENGLISH),
            DateTimeFormatter.ofPattern("M/d/yyyy h:mm a", Locale.ENGLISH),
            DateTimeFormatter.ofPattern("M/d/yyyy", Locale.ENGLISH),
            DateTimeFormatter.ofPattern("M/d/yy", Locale.ENGLISH),
            DateTimeFormatter.ofPattern("yyyy-MM-dd H:mm:ss", Locale.ENGLISH)
    );

    private final DatasetRegistryService datasetRegistryService;
    private final HdfsStorageService hdfsStorageService;
    private final AnalyticsProperties analyticsProperties;
    private final ConcurrentMap<String, CachedSnapshot> cache = new ConcurrentHashMap<>();

    public CsvAnalyticsService(DatasetRegistryService datasetRegistryService,
                               HdfsStorageService hdfsStorageService,
                               AnalyticsProperties analyticsProperties) {
        this.datasetRegistryService = datasetRegistryService;
        this.hdfsStorageService = hdfsStorageService;
        this.analyticsProperties = analyticsProperties;
    }

    public CsvAnalyticsSnapshot analyze(UUID datasetId, Integer requestedMaxFiles, boolean refresh) throws IOException {
        DatasetRegistration dataset = datasetRegistryService.getRequired(datasetId);
        if (dataset.getDatasetType() != DatasetType.CSV_TEXT) {
            throw new IllegalArgumentException("Current CSV analytics implementation supports CSV_TEXT datasets only.");
        }

        int maxFiles = resolveMaxFiles(requestedMaxFiles);
        String cacheKey = datasetId + ":" + maxFiles;
        CachedSnapshot cachedSnapshot = cache.get(cacheKey);
        if (!refresh && cachedSnapshot != null && !cachedSnapshot.isExpired(analyticsProperties.getCacheTtl().toMillis())) {
            return cachedSnapshot.snapshot();
        }

        if (!hdfsStorageService.exists(dataset.getHdfsPath())) {
            throw new IllegalArgumentException("HDFS path does not exist: " + dataset.getHdfsPath());
        }

        MutableAnalytics mutableAnalytics = new MutableAnalytics();
        List<String> filePaths = hdfsStorageService.listFilePaths(dataset.getHdfsPath(), maxFiles);
        for (String filePath : filePaths) {
            mutableAnalytics.incrementScannedFiles();
            try (InputStream inputStream = hdfsStorageService.open(filePath)) {
                analyzeFile(inputStream, filePath, mutableAnalytics);
            } catch (Exception exception) {
                mutableAnalytics.incrementFailedFiles();
            }
        }

        CsvAnalyticsSnapshot snapshot = mutableAnalytics.toSnapshot(dataset, maxFiles, Instant.now(), analyticsProperties);
        cache.put(cacheKey, new CachedSnapshot(Instant.now(), snapshot));
        return snapshot;
    }

    private void analyzeFile(InputStream inputStream, String filePath, MutableAnalytics mutableAnalytics) throws IOException {
        if (isSpreadsheetFile(filePath)) {
            analyzeSpreadsheetFile(inputStream, mutableAnalytics);
            return;
        }

        try (Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
             CSVParser parser = CSVFormat.DEFAULT.builder()
                     .setHeader()
                     .setSkipHeaderRecord(true)
                     .setIgnoreEmptyLines(true)
                     .setTrim(true)
                     .get()
                     .parse(reader)) {
            List<String> headers = parser.getHeaderNames();
            if (headers == null || headers.isEmpty()) {
                throw new IllegalArgumentException("CSV file is missing headers");
            }
            List<TabularRecord> records = parser.getRecords().stream()
                    .map(record -> new TabularRecord(record.toMap()))
                    .toList();
            FileSchema schema = detectSchema(headers, records);
            if (schema.metricColumns().isEmpty()) {
                throw new IllegalArgumentException("CSV file does not contain detectable numeric metric columns");
            }
            mutableAnalytics.acceptFile(schema, records);
        }
    }

    private void analyzeSpreadsheetFile(InputStream inputStream, MutableAnalytics mutableAnalytics) throws IOException {
        try (BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
             Workbook workbook = WorkbookFactory.create(bufferedInputStream)) {
            Sheet sheet = workbook.getNumberOfSheets() > 0 ? workbook.getSheetAt(0) : null;
            if (sheet == null) {
                throw new IllegalArgumentException("Spreadsheet file does not contain any sheets");
            }

            Row headerRow = sheet.getPhysicalNumberOfRows() > 0 ? sheet.getRow(sheet.getFirstRowNum()) : null;
            if (headerRow == null) {
                throw new IllegalArgumentException("Spreadsheet file is missing headers");
            }

            DataFormatter formatter = new DataFormatter();
            List<String> headers = new ArrayList<>();
            int lastCell = Math.max(headerRow.getLastCellNum(), (short) 0);
            for (int cellIndex = 0; cellIndex < lastCell; cellIndex++) {
                String header = formatter.formatCellValue(headerRow.getCell(cellIndex)).trim();
                headers.add(header.isBlank() ? "Column" + (cellIndex + 1) : header);
            }
            if (headers.isEmpty()) {
                throw new IllegalArgumentException("Spreadsheet file is missing headers");
            }

            List<TabularRecord> records = new ArrayList<>();
            for (int rowIndex = sheet.getFirstRowNum() + 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
                Row row = sheet.getRow(rowIndex);
                if (row == null || isBlankRow(row, headers.size(), formatter)) {
                    continue;
                }

                Map<String, String> values = new LinkedHashMap<>();
                for (int cellIndex = 0; cellIndex < headers.size(); cellIndex++) {
                    Cell cell = row.getCell(cellIndex);
                    values.put(headers.get(cellIndex), formatter.formatCellValue(cell).trim());
                }
                records.add(new TabularRecord(values));
            }

            FileSchema schema = detectSchema(headers, records);
            if (schema.metricColumns().isEmpty()) {
                throw new IllegalArgumentException("Spreadsheet file does not contain detectable numeric metric columns");
            }
            mutableAnalytics.acceptFile(schema, records);
        }
    }

    private FileSchema detectSchema(List<String> headers, List<TabularRecord> records) {
        Map<String, String> normalizedToOriginal = new LinkedHashMap<>();
        for (String header : headers) {
            normalizedToOriginal.putIfAbsent(normalizeHeader(header), header);
        }

        String dateColumn = findFirstMatch(normalizedToOriginal, DATE_COLUMN_CANDIDATES);
        String locationColumn = findFirstMatch(normalizedToOriginal, LOCATION_COLUMN_CANDIDATES);

        List<String> metricColumns = new ArrayList<>();
        for (String candidate : PREFERRED_METRIC_CANDIDATES) {
            String column = normalizedToOriginal.get(candidate);
            if (column != null && !metricColumns.contains(column)) {
                metricColumns.add(column);
            }
        }

        if (metricColumns.isEmpty()) {
            for (String header : headers) {
                if (header.equals(dateColumn) || header.equals(locationColumn)) {
                    continue;
                }
                if (isNumericColumn(header, records)) {
                    metricColumns.add(header);
                }
            }
        }

        return new FileSchema(dateColumn, locationColumn, metricColumns);
    }

    private boolean isNumericColumn(String header, List<TabularRecord> records) {
        int inspected = 0;
        for (TabularRecord record : records) {
            if (!record.isMapped(header)) {
                return false;
            }
            String value = record.get(header);
            if (value == null || value.isBlank()) {
                continue;
            }
            inspected++;
            if (parseNumericValue(value).isEmpty()) {
                return false;
            }
            if (inspected >= 25) {
                return true;
            }
        }
        return inspected > 0;
    }

    private String findFirstMatch(Map<String, String> normalizedToOriginal, List<String> candidates) {
        for (String candidate : candidates) {
            String match = normalizedToOriginal.get(candidate);
            if (match != null) {
                return match;
            }
        }
        return null;
    }

    private int resolveMaxFiles(Integer requestedMaxFiles) {
        if (requestedMaxFiles == null || requestedMaxFiles < 1) {
            return analyticsProperties.getDefaultMaxFiles();
        }
        return Math.min(requestedMaxFiles, analyticsProperties.getMaxFilesHardLimit());
    }

    private boolean isSpreadsheetFile(String filePath) {
        String normalized = filePath == null ? "" : filePath.toLowerCase(Locale.ROOT);
        return normalized.endsWith(".xls") || normalized.endsWith(".xlsx");
    }

    private boolean isBlankRow(Row row, int headerCount, DataFormatter formatter) {
        for (int cellIndex = 0; cellIndex < headerCount; cellIndex++) {
            if (!formatter.formatCellValue(row.getCell(cellIndex)).trim().isBlank()) {
                return false;
            }
        }
        return true;
    }

    private String normalizeHeader(String header) {
        return header == null ? "" : header.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_]+", "");
    }

    private static Optional<LocalDate> parseDate(String rawValue) {
        if (rawValue == null || rawValue.isBlank()) {
            return Optional.empty();
        }

        String cleaned = rawValue.trim();
        for (DateTimeFormatter formatter : DATE_FORMATTERS) {
            try {
                TemporalAccessor accessor = formatter.parseBest(
                        cleaned,
                        ZonedDateTime::from,
                        LocalDateTime::from,
                        LocalDate::from
                );

                if (accessor instanceof ZonedDateTime zonedDateTime) {
                    return Optional.of(zonedDateTime.toLocalDate());
                }
                if (accessor instanceof LocalDateTime localDateTime) {
                    return Optional.of(localDateTime.toLocalDate());
                }
                if (accessor instanceof LocalDate localDate) {
                    return Optional.of(localDate);
                }
            } catch (DateTimeParseException ignored) {
                // Try next formatter.
            }
        }

        int whitespaceIndex = cleaned.indexOf(' ');
        if (whitespaceIndex > 0) {
            return parseDate(cleaned.substring(0, whitespaceIndex));
        }
        return Optional.empty();
    }

    private static Optional<Long> parseNumericValue(String rawValue) {
        if (rawValue == null || rawValue.isBlank()) {
            return Optional.empty();
        }

        try {
            String cleaned = rawValue.trim().replace(",", "");
            return Optional.of(new BigDecimal(cleaned).longValue());
        } catch (NumberFormatException exception) {
            return Optional.empty();
        }
    }

    private static final class MutableAnalytics {

        private int scannedFiles;
        private int processedRows;
        private int failedFiles;
        private String dateColumn;
        private String locationColumn;
        private final Set<String> distinctLocations = new LinkedHashSet<>();
        private final Set<String> metricColumns = new LinkedHashSet<>();
        private final Map<LocalDate, Long> rowsByDate = new HashMap<>();
        private final Map<String, Map<LocalDate, Long>> metricByDate = new LinkedHashMap<>();
        private final Map<String, Map<String, Map<LocalDate, Long>>> metricByLocationDate = new LinkedHashMap<>();
        private final Map<String, Long> metricTotalsWithoutDate = new LinkedHashMap<>();
        private LocalDate firstObservedDate;
        private LocalDate lastObservedDate;

        void incrementScannedFiles() {
            scannedFiles++;
        }

        void incrementFailedFiles() {
            failedFiles++;
        }

        void acceptFile(FileSchema schema, List<TabularRecord> records) {
            if (dateColumn == null && schema.dateColumn() != null) {
                dateColumn = schema.dateColumn();
            }
            if (locationColumn == null && schema.locationColumn() != null) {
                locationColumn = schema.locationColumn();
            }
            metricColumns.addAll(schema.metricColumns());

            for (TabularRecord record : records) {
                processedRows++;

                LocalDate observationDate = schema.dateColumn() == null || !record.isMapped(schema.dateColumn())
                        ? null
                        : parseCsvDate(record.get(schema.dateColumn())).orElse(null);
                if (observationDate != null) {
                    rowsByDate.merge(observationDate, 1L, Long::sum);
                    if (firstObservedDate == null || observationDate.isBefore(firstObservedDate)) {
                        firstObservedDate = observationDate;
                    }
                    if (lastObservedDate == null || observationDate.isAfter(lastObservedDate)) {
                        lastObservedDate = observationDate;
                    }
                }

                String location = resolveLocation(record, schema.locationColumn());
                if (location != null) {
                    distinctLocations.add(location);
                }

                for (String metricColumn : schema.metricColumns()) {
                    if (!record.isMapped(metricColumn)) {
                        continue;
                    }

                    Long value = parseCsvNumber(record.get(metricColumn)).orElse(null);
                    if (value == null) {
                        continue;
                    }

                    metricByDate.computeIfAbsent(metricColumn, ignored -> new HashMap<>());
                    metricByLocationDate.computeIfAbsent(metricColumn, ignored -> new LinkedHashMap<>());

                    if (observationDate != null) {
                        metricByDate.get(metricColumn).merge(observationDate, value, Long::sum);
                    } else {
                        metricTotalsWithoutDate.merge(metricColumn, value, Long::sum);
                    }

                    if (location != null) {
                        LocalDate locationBucket = observationDate == null ? LocalDate.MIN : observationDate;
                        metricByLocationDate.get(metricColumn)
                                .computeIfAbsent(location, ignored -> new HashMap<>())
                                .merge(locationBucket, value, Long::sum);
                    }
                }
            }
        }

        CsvAnalyticsSnapshot toSnapshot(DatasetRegistration dataset,
                                        int maxFiles,
                                        Instant generatedAt,
                                        AnalyticsProperties analyticsProperties) {
            CsvAnalyticsOverview overview = new CsvAnalyticsOverview();
            overview.setScannedFiles(scannedFiles);
            overview.setProcessedRows(processedRows);
            overview.setFailedFiles(failedFiles);
            overview.setDistinctLocations(distinctLocations.size());
            overview.setDetectedMetrics(metricColumns.size());
            overview.setDateColumn(dateColumn);
            overview.setLocationColumn(locationColumn);
            overview.setMetricColumns(new ArrayList<>(metricColumns));
            overview.setFirstObservedAt(toInstant(firstObservedDate));
            overview.setLastObservedAt(toInstant(lastObservedDate));

            CsvAnalyticsSnapshot snapshot = new CsvAnalyticsSnapshot();
            snapshot.setDatasetId(dataset.getId());
            snapshot.setDatasetName(dataset.getName());
            snapshot.setDatasetType(dataset.getDatasetType());
            snapshot.setHdfsPath(dataset.getHdfsPath());
            snapshot.setGeneratedAt(generatedAt);
            snapshot.setMaxFiles(maxFiles);
            snapshot.setOverview(overview);
            snapshot.setRowsByDate(toRowsByDateSeries());
            snapshot.setMetricTimeSeries(toMetricTimeSeries());
            snapshot.setMetricTotals(toMetricTotals());
            snapshot.setTopLocationsByMetric(toTopLocationsByMetric(analyticsProperties.getDefaultTopLimit()));
            return snapshot;
        }

        private List<TimeSeriesPoint> toRowsByDateSeries() {
            return rowsByDate.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(entry -> new TimeSeriesPoint(entry.getKey().toString(), entry.getValue()))
                    .toList();
        }

        private List<MetricSeries> toMetricTimeSeries() {
            return metricByDate.entrySet()
                    .stream()
                    .filter(entry -> !entry.getValue().isEmpty())
                    .map(entry -> new MetricSeries(
                            entry.getKey(),
                            entry.getValue().entrySet()
                                    .stream()
                                    .sorted(Map.Entry.comparingByKey())
                                    .map(point -> new TimeSeriesPoint(point.getKey().toString(), point.getValue()))
                                    .toList()
                    ))
                    .toList();
        }

        private List<NamedCount> toMetricTotals() {
            LocalDate latestDate = lastObservedDate;
            return metricColumns.stream()
                    .map(metric -> new NamedCount(metric, latestDate == null
                            ? metricTotalsWithoutDate.getOrDefault(metric, 0L)
                            : metricByDate.getOrDefault(metric, Map.of()).getOrDefault(latestDate, 0L)))
                    .sorted(countComparator())
                    .toList();
        }

        private List<MetricBreakdown> toTopLocationsByMetric(int limit) {
            LocalDate latestDate = lastObservedDate;
            return metricColumns.stream()
                    .map(metric -> {
                        Map<String, Map<LocalDate, Long>> locationSeries = metricByLocationDate.getOrDefault(metric, Map.of());
                        List<NamedCount> items = locationSeries.entrySet()
                                .stream()
                                .map(entry -> new NamedCount(entry.getKey(), latestDate == null
                                        ? entry.getValue().getOrDefault(LocalDate.MIN, 0L)
                                        : entry.getValue().getOrDefault(latestDate, 0L)))
                                .filter(item -> item.getCount() > 0)
                                .sorted(countComparator())
                                .limit(Math.max(1, limit))
                                .toList();
                        return new MetricBreakdown(metric, items);
                    })
                    .filter(breakdown -> !breakdown.getItems().isEmpty())
                    .toList();
        }

        private Comparator<NamedCount> countComparator() {
            return Comparator.comparingLong(NamedCount::getCount)
                    .reversed()
                    .thenComparing(NamedCount::getName);
        }

        private Instant toInstant(LocalDate date) {
            return date == null ? null : date.atStartOfDay().toInstant(ZoneOffset.UTC);
        }
    }

    private static Optional<LocalDate> parseCsvDate(String rawValue) {
        return parseDate(rawValue);
    }

    private static Optional<Long> parseCsvNumber(String rawValue) {
        return parseNumericValue(rawValue);
    }

    private static String resolveLocation(TabularRecord record, String locationColumn) {
        if (locationColumn == null || !record.isMapped(locationColumn)) {
            return null;
        }

        String location = record.get(locationColumn);
        if (location == null || location.isBlank()) {
            return null;
        }
        return location.trim();
    }

    private record FileSchema(String dateColumn, String locationColumn, List<String> metricColumns) {
    }

    private record TabularRecord(Map<String, String> values) {
        String get(String column) {
            return values.get(column);
        }

        boolean isMapped(String column) {
            return values.containsKey(column);
        }
    }

    private record CachedSnapshot(Instant cachedAt, CsvAnalyticsSnapshot snapshot) {
        boolean isExpired(long ttlMillis) {
            return cachedAt.plusMillis(ttlMillis).isBefore(Instant.now());
        }
    }
}
