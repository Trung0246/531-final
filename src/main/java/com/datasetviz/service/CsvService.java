package com.datasetviz.service;

import com.datasetviz.config.AnalyticsProps;
import com.datasetviz.dto.DashboardProgressEvent;
import com.datasetviz.dto.DashboardView;
import com.datasetviz.model.ColumnProfile;
import com.datasetviz.model.CsvAnalyticsOverview;
import com.datasetviz.model.CsvAnalyticsSnapshot;
import com.datasetviz.model.DatasetRegistration;
import com.datasetviz.model.DatasetType;
import com.datasetviz.model.MetricBreakdown;
import com.datasetviz.model.MetricSeries;
import com.datasetviz.model.NamedCount;
import com.datasetviz.model.TimeSeriesPoint;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
import java.util.Iterator;
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
public class CsvService {

    private static final int SCHEMA_SAMPLE_SIZE = 100;
    private static final int COLUMN_SAMPLE_LIMIT = 10;
    private static final int COLUMN_TOP_VALUE_LIMIT = 10;
    private static final int COLUMN_HISTOGRAM_BUCKETS = 8;
    private static final int COLUMN_NUMERIC_SAMPLE_LIMIT = 10_000;
    private static final int ROW_PROGRESS_INTERVAL = 25_000;
    private static final int MIN_ROW_PROGRESS_INTERVAL = 50;
    private static final int MAX_ROW_PROGRESS_INTERVAL = 1_000_000;
    private static final int DASHBOARD_PROGRESS_INTERVAL = 500;

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

    private final RegistryService registryService;
    private final HdfsStore hdfsStore;
    private final AnalyticsProps analyticsProps;
    private final ProgressService progressService;
    private final ViewService viewService;
    private final StateService stateService;
    private final ConcurrentMap<String, CachedSnapshot> cache = new ConcurrentHashMap<>();

    public CsvService(RegistryService registryService,
                               HdfsStore hdfsStore,
                               AnalyticsProps analyticsProps) {
        this(registryService, hdfsStore, analyticsProps, new ProgressService(), new ViewService(), new StateService());
    }

    public CsvService(RegistryService registryService,
                               HdfsStore hdfsStore,
                               AnalyticsProps analyticsProps,
                               ProgressService progressService) {
        this(registryService, hdfsStore, analyticsProps, progressService, new ViewService(), new StateService());
    }

    @Autowired
    public CsvService(RegistryService registryService,
                               HdfsStore hdfsStore,
                               AnalyticsProps analyticsProps,
                               ProgressService progressService,
                               ViewService viewService,
                               StateService stateService) {
        this.registryService = registryService;
        this.hdfsStore = hdfsStore;
        this.analyticsProps = analyticsProps;
        this.progressService = progressService;
        this.viewService = viewService;
        this.stateService = stateService;
    }

    public CsvAnalyticsSnapshot analyze(UUID datasetId, Integer requestedMaxFiles, boolean refresh) throws IOException {
        return analyze(datasetId, requestedMaxFiles, null, refresh);
    }

    public void invalidateCache(UUID datasetId) {
        String cacheKeyPrefix = datasetId + ":";
        cache.keySet().removeIf(key -> key.startsWith(cacheKeyPrefix));
        progressService.clear(datasetId.toString());
    }

    public CsvAnalyticsSnapshot analyze(UUID datasetId, Integer requestedMaxFiles, Integer requestedUpdateEveryRows, boolean refresh) throws IOException {
        return analyze(datasetId, requestedMaxFiles, requestedUpdateEveryRows, null, refresh);
    }

    public CsvAnalyticsSnapshot analyze(UUID datasetId,
                                        Integer requestedMaxFiles,
                                        Integer requestedUpdateEveryRows,
                                        Integer requestedFullDashboardUpdateEveryRows,
                                        boolean refresh) throws IOException {
        DatasetRegistration dataset = registryService.getRequired(datasetId);
        if (dataset.getDatasetType() != DatasetType.CSV_TEXT) {
            throw new IllegalArgumentException("Current CSV analytics implementation supports CSV_TEXT datasets only.");
        }

        int maxFiles = fileLimit(requestedMaxFiles);
        int updateEveryRows = rowStep(requestedUpdateEveryRows);
        int fullDashboardUpdateEveryRows = dashStep(requestedFullDashboardUpdateEveryRows);
        String cacheKey = datasetId + ":" + maxFiles;
        CachedSnapshot cachedSnapshot = cache.get(cacheKey);
        if (!refresh && cachedSnapshot != null && !cachedSnapshot.isExpired(analyticsProps.getCacheTtl().toMillis())) {
            CsvAnalyticsSnapshot snapshot = cachedSnapshot.snapshot();
            emit(datasetId, "cache", "Loaded dashboard analytics from cache.", snapshot.getOverview().getScannedFiles(), snapshot.getOverview().getScannedFiles(), snapshot.getOverview().getProcessedRows(), snapshot.getOverview().getFailedFiles(), List.of(), viewService.toDashboardCharts(snapshot), viewService.toDashboardView(snapshot), true);
            return snapshot;
        }

        StateService.Job job = stateService.start(datasetId);
        try {
            emit(datasetId, "starting", "Preparing CSV/text analytics.", 0, 0, 0, 0, List.of(), List.of(), null, false);
            if (!hdfsStore.exists(dataset.getHdfsPath())) {
                throw new IllegalArgumentException("HDFS path does not exist: " + dataset.getHdfsPath());
            }

            MutableAnalytics mutableAnalytics = new MutableAnalytics();
            List<String> filePaths = hdfsStore.listFilePaths(dataset.getHdfsPath(), maxFiles);
            List<MutableFileProgress> fileProgress = newFileProgress(filePaths);
            emit(datasetId, "listed", "Found " + filePaths.size() + " file(s) to scan.", 0, filePaths.size(), 0, 0, fileProgressView(fileProgress), List.of(), null, false);
            for (int fileIndex = 0; fileIndex < filePaths.size(); fileIndex++) {
                job.throwIfCancelled();
                String filePath = filePaths.get(fileIndex);
                MutableFileProgress currentFile = fileProgress.get(fileIndex);
                mutableAnalytics.incrementScannedFiles();
                currentFile.start("Scanning file.");
                emitProgress(datasetId, dataset, "processing", "Scanning " + fileName(filePath), mutableAnalytics, filePaths.size(), fileProgress, false, false, false);
                try (StateService.Lock ignored = stateService.lock(filePath);
                     InputStream inputStream = hdfsStore.open(filePath)) {
                    analyzeFile(job, datasetId, dataset, inputStream, filePath, mutableAnalytics, filePaths.size(), currentFile, fileProgress, updateEveryRows, fullDashboardUpdateEveryRows);
                    currentFile.complete("Finished file.");
                    emitProgress(datasetId, dataset, "processing", "Finished " + fileName(filePath), mutableAnalytics, filePaths.size(), fileProgress, false, true, true);
                } catch (CancelledException exception) {
                    currentFile.fail("Cancelled.");
                    emitProgress(datasetId, dataset, "cancelled", "Dashboard analytics cancelled.", mutableAnalytics, filePaths.size(), fileProgress, true, true, true);
                    throw exception;
                } catch (Exception exception) {
                    mutableAnalytics.incrementFailedFiles();
                    currentFile.fail(exception.getMessage());
                    emitProgress(datasetId, dataset, "warning", "Skipped " + fileName(filePath) + ": " + exception.getMessage(), mutableAnalytics, filePaths.size(), fileProgress, false, true, true);
                }
            }

            CsvAnalyticsSnapshot snapshot = mutableAnalytics.toSnapshot(dataset, maxFiles, Instant.now(), analyticsProps);
            cache.put(cacheKey, new CachedSnapshot(Instant.now(), snapshot));
            emit(datasetId, "complete", "Dashboard analytics ready.", snapshot.getOverview().getScannedFiles(), filePaths.size(), snapshot.getOverview().getProcessedRows(), snapshot.getOverview().getFailedFiles(), fileProgressView(fileProgress), viewService.toDashboardCharts(snapshot), viewService.toDashboardView(snapshot), true);
            return snapshot;
        } finally {
            stateService.finish(job);
        }
    }

    private void analyzeFile(StateService.Job job,
                             UUID datasetId,
                             DatasetRegistration dataset,
                             InputStream inputStream,
                             String filePath,
                             MutableAnalytics mutableAnalytics,
                             int totalFiles,
                             MutableFileProgress currentFile,
                             List<MutableFileProgress> fileProgress,
                             int updateEveryRows,
                             int fullDashboardUpdateEveryRows) throws IOException {
        job.throwIfCancelled();
        if (isSheet(filePath)) {
            int beforeRows = mutableAnalytics.getProcessedRows();
            analyzeSheet(job, inputStream, mutableAnalytics);
            currentFile.processedRows += mutableAnalytics.getProcessedRows() - beforeRows;
            emitProgress(datasetId, dataset, "processing", "Processed " + currentFile.processedRows + " rows from " + fileName(filePath), mutableAnalytics, totalFiles, fileProgress, false, true, true);
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
            List<TabularRecord> sampleRecords = new ArrayList<>();
            Iterator<CSVRecord> iterator = parser.iterator();
            while (iterator.hasNext() && sampleRecords.size() < SCHEMA_SAMPLE_SIZE) {
                sampleRecords.add(new TabularRecord(iterator.next()));
            }

            FileSchema schema = detectSchema(headers, sampleRecords);
            if (schema.metricColumns().isEmpty()) {
                throw new IllegalArgumentException("CSV file does not contain detectable numeric metric columns");
            }

            mutableAnalytics.acceptSchema(schema);
            for (TabularRecord record : sampleRecords) {
                mutableAnalytics.acceptRecord(schema, record);
                currentFile.processedRows++;
            }
            emitProgress(datasetId, dataset, "processing", "Processed " + currentFile.processedRows + " rows from " + fileName(filePath), mutableAnalytics, totalFiles, fileProgress, false, true, true);

            while (iterator.hasNext()) {
                job.throwIfCancelled();
                mutableAnalytics.acceptRecord(schema, new TabularRecord(iterator.next()));
                currentFile.processedRows++;
                boolean includeCharts = mutableAnalytics.getProcessedRows() % updateEveryRows == 0;
                boolean includeDashboard = mutableAnalytics.getProcessedRows() % fullDashboardUpdateEveryRows == 0;
                if (includeCharts || includeDashboard) {
                    emitProgress(datasetId, dataset, "processing", "Processed " + currentFile.processedRows + " rows from " + fileName(filePath), mutableAnalytics, totalFiles, fileProgress, false, includeCharts || includeDashboard, includeDashboard);
                }
            }
        }
    }

    private void analyzeSheet(StateService.Job job, InputStream inputStream, MutableAnalytics mutableAnalytics) throws IOException {
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
                job.throwIfCancelled();
                Row row = sheet.getRow(rowIndex);
                if (row == null || blankRow(row, headers.size(), formatter)) {
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

    private FileSchema detectSchema(List<String> headers, List<?> records) {
        Map<String, String> normalizedToOriginal = new LinkedHashMap<>();
        for (String header : headers) {
            normalizedToOriginal.putIfAbsent(normHeader(header), header);
        }

        String dateColumn = firstMatch(normalizedToOriginal, DATE_COLUMN_CANDIDATES);
        String locationColumn = firstMatch(normalizedToOriginal, LOCATION_COLUMN_CANDIDATES);

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

        return new FileSchema(dateColumn, locationColumn, metricColumns, headers);
    }

    private boolean isNumericColumn(String header, List<?> records) {
        int inspected = 0;
        for (Object rawRecord : records) {
            TabularRecord record = asTabularRecord(rawRecord);
            if (!record.isMapped(header)) {
                return false;
            }
            String value = record.get(header);
            if (value == null || value.isBlank()) {
                continue;
            }
            inspected++;
            if (parseNum(value).isEmpty()) {
                return false;
            }
            if (inspected >= 25) {
                return true;
            }
        }
        return inspected > 0;
    }

    private String firstMatch(Map<String, String> normalizedToOriginal, List<String> candidates) {
        for (String candidate : candidates) {
            String match = normalizedToOriginal.get(candidate);
            if (match != null) {
                return match;
            }
        }
        return null;
    }

    private int fileLimit(Integer requestedMaxFiles) {
        if (requestedMaxFiles == null || requestedMaxFiles < 1) {
            return analyticsProps.getDefaultMaxFiles();
        }
        return Math.min(requestedMaxFiles, analyticsProps.getMaxFilesHardLimit());
    }

    private int rowStep(Integer requestedUpdateEveryRows) {
        if (requestedUpdateEveryRows == null || requestedUpdateEveryRows < 1) {
            return ROW_PROGRESS_INTERVAL;
        }
        return Math.min(Math.max(requestedUpdateEveryRows, MIN_ROW_PROGRESS_INTERVAL), MAX_ROW_PROGRESS_INTERVAL);
    }

    private int dashStep(Integer requestedFullDashboardUpdateEveryRows) {
        if (requestedFullDashboardUpdateEveryRows == null || requestedFullDashboardUpdateEveryRows < 1) {
            return DASHBOARD_PROGRESS_INTERVAL;
        }
        return Math.min(Math.max(requestedFullDashboardUpdateEveryRows, MIN_ROW_PROGRESS_INTERVAL), MAX_ROW_PROGRESS_INTERVAL);
    }

    private boolean isSheet(String filePath) {
        String normalized = filePath == null ? "" : filePath.toLowerCase(Locale.ROOT);
        return normalized.endsWith(".xls") || normalized.endsWith(".xlsx");
    }

    private boolean blankRow(Row row, int headerCount, DataFormatter formatter) {
        for (int cellIndex = 0; cellIndex < headerCount; cellIndex++) {
            if (!formatter.formatCellValue(row.getCell(cellIndex)).trim().isBlank()) {
                return false;
            }
        }
        return true;
    }

    private String normHeader(String header) {
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

    private static Optional<Long> parseNum(String rawValue) {
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

    private void emitProgress(UUID datasetId,
                                  DatasetRegistration dataset,
                                  String stage,
                                  String message,
                                  MutableAnalytics analytics,
                                  int totalFiles,
                                  List<MutableFileProgress> files,
                                  boolean complete,
                                  boolean includeCharts,
                                  boolean includeDashboard) {
        CsvAnalyticsSnapshot snapshot = includeCharts || includeDashboard
                ? analytics.toSnapshot(dataset, totalFiles, Instant.now(), analyticsProps)
                : null;
        List<DashboardView.Chart> charts = includeCharts && snapshot != null
                ? viewService.toDashboardCharts(snapshot)
                : List.of();
        DashboardView partialDashboard = includeDashboard && snapshot != null
                ? viewService.toDashboardView(snapshot)
                : null;
        emit(datasetId, stage, message, analytics.getScannedFiles(), totalFiles, analytics.getProcessedRows(), analytics.getFailedFiles(), fileProgressView(files), charts, partialDashboard, complete);
    }

    private void emit(UUID datasetId,
                         String stage,
                         String message,
                         int scannedFiles,
                         int totalFiles,
                         int processedRows,
                         int failedFiles,
                         List<DashboardProgressEvent.FileProgress> files,
                         List<DashboardView.Chart> charts,
                         DashboardView dashboard,
                         boolean complete) {
        progressService.emit(new DashboardProgressEvent(
                datasetId.toString(),
                stage,
                message,
                scannedFiles,
                totalFiles,
                processedRows,
                failedFiles,
                files,
                charts,
                dashboard,
                complete
        ));
    }

    private List<MutableFileProgress> newFileProgress(List<String> filePaths) {
        return filePaths.stream()
                .map(path -> new MutableFileProgress(path, fileName(path)))
                .toList();
    }

    private List<DashboardProgressEvent.FileProgress> fileProgressView(List<MutableFileProgress> files) {
        return files.stream()
                .map(file -> new DashboardProgressEvent.FileProgress(file.path, file.name, file.status, file.processedRows, file.message))
                .toList();
    }

    private String fileName(String filePath) {
        int slashIndex = filePath == null ? -1 : filePath.lastIndexOf('/');
        return slashIndex >= 0 ? filePath.substring(slashIndex + 1) : String.valueOf(filePath);
    }

    private static final class MutableFileProgress {
        private final String path;
        private final String name;
        private String status = "queued";
        private int processedRows;
        private String message = "Waiting to scan.";

        private MutableFileProgress(String path, String name) {
            this.path = path;
            this.name = name;
        }

        private void start(String message) {
            this.status = "processing";
            this.message = message;
        }

        private void complete(String message) {
            this.status = "complete";
            this.message = message;
        }

        private void fail(String message) {
            this.status = "failed";
            this.message = message == null || message.isBlank() ? "Failed to scan file." : message;
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
        private final Map<String, ColumnAccumulator> columnAccumulators = new LinkedHashMap<>();
        private LocalDate firstObservedDate;
        private LocalDate lastObservedDate;

        void incrementScannedFiles() {
            scannedFiles++;
        }

        void incrementFailedFiles() {
            failedFiles++;
        }

        int getScannedFiles() {
            return scannedFiles;
        }

        int getProcessedRows() {
            return processedRows;
        }

        int getFailedFiles() {
            return failedFiles;
        }

        void acceptFile(FileSchema schema, List<?> records) {
            acceptSchema(schema);
            for (Object rawRecord : records) {
                acceptRecord(schema, rawRecord);
            }
        }

        void acceptSchema(FileSchema schema) {
            if (dateColumn == null && schema.dateColumn() != null) {
                dateColumn = schema.dateColumn();
            }
            if (locationColumn == null && schema.locationColumn() != null) {
                locationColumn = schema.locationColumn();
            }
            metricColumns.addAll(schema.metricColumns());
            schema.headers().forEach(header -> columnAccumulators.computeIfAbsent(header, ColumnAccumulator::new));
        }

        void acceptRecord(FileSchema schema, Object rawRecord) {
            TabularRecord record = asTabularRecord(rawRecord);
            processedRows++;

            for (String header : schema.headers()) {
                columnAccumulators.computeIfAbsent(header, ColumnAccumulator::new)
                        .accept(record.isMapped(header) ? record.get(header) : null);
            }

            LocalDate observationDate = schema.dateColumn() == null || !record.isMapped(schema.dateColumn())
                    ? null
                    : csvDate(record.get(schema.dateColumn())).orElse(null);
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

                Long value = csvNum(record.get(metricColumn)).orElse(null);
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

        CsvAnalyticsSnapshot toSnapshot(DatasetRegistration dataset,
                                        int maxFiles,
                                        Instant generatedAt,
                                        AnalyticsProps analyticsProps) {
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
            snapshot.setTopLocationsByMetric(toTopLocationsByMetric(analyticsProps.getDefaultTopLimit()));
            snapshot.setColumnProfiles(toColumnProfiles());
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

        private List<ColumnProfile> toColumnProfiles() {
            return columnAccumulators.values().stream()
                    .map(ColumnAccumulator::toProfile)
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

    private static Optional<LocalDate> csvDate(String rawValue) {
        return parseDate(rawValue);
    }

    private static Optional<Long> csvNum(String rawValue) {
        return parseNum(rawValue);
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

    private static String resolveLocation(CSVRecord record, String locationColumn) {
        return resolveLocation(asTabularRecord(record), locationColumn);
    }

    private static TabularRecord asTabularRecord(Object rawRecord) {
        if (rawRecord instanceof TabularRecord tabularRecord) {
            return tabularRecord;
        }
        if (rawRecord instanceof CSVRecord csvRecord) {
            return new TabularRecord(csvRecord.toMap());
        }
        throw new IllegalArgumentException("Unsupported tabular record type: " + rawRecord);
    }

    private record FileSchema(String dateColumn, String locationColumn, List<String> metricColumns, List<String> headers) {
        private FileSchema(String dateColumn, String locationColumn, List<String> metricColumns) {
            this(dateColumn, locationColumn, metricColumns, List.of());
        }
    }

    private static final class ColumnAccumulator {
        private final String name;
        private final List<String> sampleValues = new ArrayList<>();
        private final Map<String, Long> valueCounts = new LinkedHashMap<>();
        private final List<Long> numericSamples = new ArrayList<>();
        private int blankValues;
        private int nonBlankValues;
        private int numericValues;
        private int dateValues;

        private ColumnAccumulator(String name) {
            this.name = name;
        }

        private void accept(String rawValue) {
            if (rawValue == null || rawValue.isBlank()) {
                blankValues++;
                return;
            }

            String value = rawValue.trim();
            nonBlankValues++;
            valueCounts.merge(value, 1L, Long::sum);
            Long numericValue = csvNum(value).orElse(null);
            if (numericValue != null) {
                numericValues++;
                if (numericSamples.size() < COLUMN_NUMERIC_SAMPLE_LIMIT) {
                    numericSamples.add(numericValue);
                }
            }
            if (csvDate(value).isPresent()) {
                dateValues++;
            }
            if (sampleValues.size() < COLUMN_SAMPLE_LIMIT) {
                sampleValues.add(value);
            }
        }

        private ColumnProfile toProfile() {
            return new ColumnProfile(
                    name,
                    inferredType(),
                    List.copyOf(sampleValues),
                    blankValues,
                    nonBlankValues,
                    valueCounts.size(),
                    topValues(),
                    histogramBuckets()
            );
        }

        private List<NamedCount> topValues() {
            return valueCounts.entrySet().stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue().reversed().thenComparing(Map.Entry.comparingByKey()))
                    .limit(COLUMN_TOP_VALUE_LIMIT)
                    .map(entry -> new NamedCount(entry.getKey(), entry.getValue()))
                    .toList();
        }

        private List<NamedCount> histogramBuckets() {
            if (numericSamples.isEmpty() || numericValues != nonBlankValues) {
                return List.of();
            }
            long min = numericSamples.stream().mapToLong(Long::longValue).min().orElse(0L);
            long max = numericSamples.stream().mapToLong(Long::longValue).max().orElse(0L);
            if (min == max) {
                return List.of(new NamedCount(String.valueOf(min), numericSamples.size()));
            }

            int bucketCount = Math.min(COLUMN_HISTOGRAM_BUCKETS, Math.max(1, numericSamples.size()));
            long[] counts = new long[bucketCount];
            double width = (max - min) / (double) bucketCount;
            for (Long value : numericSamples) {
                int index = (int) Math.floor((value - min) / width);
                counts[Math.min(bucketCount - 1, Math.max(0, index))]++;
            }

            List<NamedCount> buckets = new ArrayList<>();
            for (int index = 0; index < bucketCount; index++) {
                long start = Math.round(min + width * index);
                long end = index == bucketCount - 1 ? max : Math.round(min + width * (index + 1));
                buckets.add(new NamedCount(start == end ? String.valueOf(start) : start + "-" + end, counts[index]));
            }
            return buckets;
        }

        private String inferredType() {
            if (nonBlankValues == 0) {
                return "EMPTY";
            }
            if (numericValues == nonBlankValues) {
                return "NUMBER";
            }
            if (dateValues == nonBlankValues) {
                return "DATE";
            }
            if (numericValues > 0 || dateValues > 0) {
                return "MIXED";
            }
            return "TEXT";
        }
    }

    private static final class TabularRecord {
        private final Map<String, String> values;
        private final CSVRecord csvRecord;

        private TabularRecord(Map<String, String> values) {
            this.values = values;
            this.csvRecord = null;
        }

        private TabularRecord(CSVRecord csvRecord) {
            this.values = null;
            this.csvRecord = csvRecord;
        }

        String get(String column) {
            return csvRecord != null ? csvRecord.get(column) : values.get(column);
        }

        boolean isMapped(String column) {
            return csvRecord != null ? csvRecord.isMapped(column) : values.containsKey(column);
        }
    }

    private record CachedSnapshot(Instant cachedAt, CsvAnalyticsSnapshot snapshot) {
        boolean isExpired(long ttlMillis) {
            return cachedAt.plusMillis(ttlMillis).isBefore(Instant.now());
        }
    }
}
