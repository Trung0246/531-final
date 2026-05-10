package com.datasetviz.service;

import com.datasetviz.config.AnalyticsProps;
import com.datasetviz.config.HdfsProps;
import com.datasetviz.dto.DashboardProgressEvent;
import com.datasetviz.model.AnalyticsOverview;
import com.datasetviz.model.CommunicationEdge;
import com.datasetviz.model.DatasetRegistration;
import com.datasetviz.model.DatasetType;
import com.datasetviz.model.EmailAnalyticsSnapshot;
import com.datasetviz.model.EmailRecord;
import com.datasetviz.model.NamedCount;
import com.datasetviz.model.TimeSeriesPoint;
import com.datasetviz.util.PathUtils;
import com.datasetviz.util.TextAnalyticsUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

@Service
public class EmailService {

    private static final DateTimeFormatter HOUR_BUCKET_FORMATTER = DateTimeFormatter.ofPattern("HH");
    private static final int LIVE_FILE_PROGRESS_LIMIT = 50;

    private final RegistryService registryService;
    private final HdfsStore hdfsStore;
    private final MailParser mailParser;
    private final AnalyticsProps analyticsProps;
    private final HdfsProps hdfsProps;
    private final ProgressService progressService;
    private final StateService stateService;
    private final ConcurrentMap<String, CachedSnapshot> cache = new ConcurrentHashMap<>();

    public EmailService(RegistryService registryService,
                                   HdfsStore hdfsStore,
                                   MailParser mailParser,
                                   AnalyticsProps analyticsProps) {
        this(registryService, hdfsStore, mailParser, analyticsProps, new HdfsProps(), new ProgressService(), new StateService());
    }

    @Autowired
    public EmailService(RegistryService registryService,
                                 HdfsStore hdfsStore,
                                 MailParser mailParser,
                                 AnalyticsProps analyticsProps,
                                 HdfsProps hdfsProps,
                                 ProgressService progressService,
                                 StateService stateService) {
        this.registryService = registryService;
        this.hdfsStore = hdfsStore;
        this.mailParser = mailParser;
        this.analyticsProps = analyticsProps;
        this.hdfsProps = hdfsProps;
        this.progressService = progressService;
        this.stateService = stateService;
    }

    public void invalidateCache(UUID datasetId) {
        String prefix = datasetId + ":";
        cache.keySet().removeIf(key -> key.startsWith(prefix));
        progressService.clear(datasetId.toString());
    }

    public EmailAnalyticsSnapshot analyze(UUID datasetId, Integer requestedMaxFiles, boolean refresh) throws IOException {
        DatasetRegistration dataset = registryService.getRequired(datasetId);
        if (dataset.getDatasetType() != DatasetType.EMAIL_ARCHIVE) {
            throw new IllegalArgumentException("Current analytics implementation supports EMAIL_ARCHIVE datasets only.");
        }

        int maxFiles = fileLimit(requestedMaxFiles);
        String cacheKey = datasetId + ":" + maxFiles;
        CachedSnapshot cachedSnapshot = cache.get(cacheKey);
        if (!refresh && cachedSnapshot != null && !cachedSnapshot.isExpired(analyticsProps.getCacheTtl().toMillis())) {
            AnalyticsOverview overview = cachedSnapshot.snapshot().getOverview();
            emit(datasetId, "cache", "Loaded dashboard analytics from cache.", overview.getScannedFiles(), overview.getScannedFiles(), overview.getParsedEmails(), overview.getFailedFiles(), true);
            return cachedSnapshot.snapshot();
        }

        StateService.Job job = stateService.start(datasetId);
        try {
            emit(datasetId, "starting", "Preparing email analytics.", 0, 0, 0, 0, false);
            materializeImport(dataset);
            if (!hdfsStore.exists(dataset.getHdfsPath())) {
                throw new IllegalArgumentException("HDFS path does not exist: " + dataset.getHdfsPath());
            }

            MutableAnalytics mutableAnalytics = new MutableAnalytics();
            List<String> filePaths = hdfsStore.listFilePaths(dataset.getHdfsPath(), maxFiles);
            List<DashboardProgressEvent.FileProgress> fileProgress = new ArrayList<>();
            filePaths.stream()
                    .limit(LIVE_FILE_PROGRESS_LIMIT)
                    .forEach(filePath -> addFileProgress(fileProgress, filePath, "queued", 0, "Waiting to scan."));
            emit(datasetId, "listed", "Found " + filePaths.size() + " file(s) to scan.", 0, filePaths.size(), 0, 0, fileProgress, false);
            for (String filePath : filePaths) {
                job.throwIfCancelled();
                mutableAnalytics.incrementScannedFiles();
                addFileProgress(fileProgress, filePath, "processing", 0, "Scanning file.");
                emitProgress(datasetId, "processing", "Scanning " + fileName(filePath), mutableAnalytics, filePaths.size(), fileProgress, false);
                try (StateService.Lock ignored = stateService.lock(filePath);
                     InputStream inputStream = hdfsStore.open(filePath)) {
                    job.throwIfCancelled();
                    EmailRecord record = mailParser.parse(inputStream, filePath).orElse(null);
                    job.throwIfCancelled();
                    if (record == null) {
                        mutableAnalytics.incrementFailedFiles();
                        addFileProgress(fileProgress, filePath, "failed", 0, "Skipped empty or invalid email file.");
                        emitProgress(datasetId, "warning", "Skipped " + fileName(filePath), mutableAnalytics, filePaths.size(), fileProgress, false);
                        continue;
                    }
                    record.setMailboxOwner(PathUtils.deriveMailboxOwner(dataset.getHdfsPath(), filePath));
                    mutableAnalytics.accept(record);
                    addFileProgress(fileProgress, filePath, "complete", 1, "Parsed email.");
                    emitProgress(datasetId, "processing", "Finished " + fileName(filePath), mutableAnalytics, filePaths.size(), fileProgress, false);
                } catch (CancelledException exception) {
                    addFileProgress(fileProgress, filePath, "failed", 0, "Cancelled.");
                    emitProgress(datasetId, "cancelled", "Dashboard analytics cancelled.", mutableAnalytics, filePaths.size(), fileProgress, true);
                    throw exception;
                } catch (Exception exception) {
                    mutableAnalytics.incrementFailedFiles();
                    addFileProgress(fileProgress, filePath, "failed", 0, exception.getMessage());
                    emitProgress(datasetId, "warning", "Skipped " + fileName(filePath) + ": " + exception.getMessage(), mutableAnalytics, filePaths.size(), fileProgress, false);
                }
            }

            EmailAnalyticsSnapshot snapshot = mutableAnalytics.toSnapshot(dataset, maxFiles, Instant.now(), analyticsProps);
            cache.put(cacheKey, new CachedSnapshot(Instant.now(), snapshot));
            emit(datasetId, "complete", "Dashboard analytics ready.", snapshot.getOverview().getScannedFiles(), filePaths.size(), snapshot.getOverview().getParsedEmails(), snapshot.getOverview().getFailedFiles(), fileProgress, true);
            return snapshot;
        } finally {
            stateService.finish(job);
        }
    }

    private void materializeImport(DatasetRegistration dataset) throws IOException {
        List<DatasetRegistration.PendingLocalImport> pendingImports = dataset.getPendingLocalImports();
        if (pendingImports.isEmpty()) {
            return;
        }

        List<DashboardProgressEvent.FileProgress> importProgress = new ArrayList<>();
        int copiedFiles = 0;
        emit(dataset.getId(), "importing", "Materializing " + pendingImports.size() + " deferred email archive import(s).", 0, 0, 0, 0, false);

        for (DatasetRegistration.PendingLocalImport pendingImport : pendingImports) {
            Path localDirectory = Paths.get(pendingImport.getLocalPath()).normalize();
            if (!Files.exists(localDirectory) || !Files.isDirectory(localDirectory)) {
                throw new IllegalArgumentException("Pending local import directory does not exist: " + localDirectory);
            }
            localDirectory = localDirectory.toRealPath();
            String targetHdfsPath = PathUtils.resolveDatasetFilePath(dataset.getHdfsPath(), pendingImport.getTargetSubdirectory());
            hdfsStore.createDirectories(targetHdfsPath);

            try (Stream<Path> stream = Files.walk(localDirectory)) {
                java.util.Iterator<Path> files = stream.filter(Files::isRegularFile).iterator();
                while (files.hasNext()) {
                    Path file = files.next();
                    String relativePath = localDirectory.relativize(file).toString().replace('\\', '/');
                    String targetPath = PathUtils.resolveHdfsPath(targetHdfsPath, relativePath);
                    addFileProgress(importProgress, targetPath, "processing", 0, "Copying from " + localDirectory + ".");
                    emit(dataset.getId(), "importing", "Copying " + localDirectory + "/" + relativePath, copiedFiles, Math.max(copiedFiles, 1), 0, 0, importProgress, false);
                    mirrorFile(file, targetPath);
                    hdfsStore.copyLocalFileToHdfs(file, targetPath);
                    copiedFiles++;
                    addFileProgress(importProgress, targetPath, "complete", 0, "Copied from " + localDirectory + ".");
                    if (copiedFiles <= 10 || copiedFiles % 25 == 0) {
                        emit(dataset.getId(), "importing", "Copied " + copiedFiles + " file(s) into HDFS.", copiedFiles, copiedFiles, 0, 0, importProgress, false);
                    }
                }
            }
        }
        emit(dataset.getId(), "importing", "Copied " + copiedFiles + " email archive file(s) into HDFS.", copiedFiles, copiedFiles, 0, 0, importProgress, false);
        registryService.clearPendingLocalImport(dataset.getId());
        dataset.clearPendingLocalImports();
    }

    private void mirrorFile(Path sourcePath, String hdfsPath) throws IOException {
        Path mirrorPath = mirrorPath(hdfsPath);
        if (mirrorPath == null) {
            return;
        }
        Files.createDirectories(mirrorPath.getParent());
        Files.copy(sourcePath, mirrorPath, StandardCopyOption.REPLACE_EXISTING);
    }

    private Path mirrorPath(String hdfsPath) throws IOException {
        if (!hdfsProps.getEmbedded().isEnabled()) {
            return null;
        }
        java.io.File mirrorDir = hdfsProps.getEmbedded().getMirrorDir();
        if (mirrorDir == null) {
            mirrorDir = new java.io.File(".hdfs-mirror");
        }
        Path mirrorRoot = mirrorDir.toPath().toAbsolutePath().normalize();
        String normalizedHdfsPath = PathUtils.normalizeHdfsPath(hdfsPath);
        if (normalizedHdfsPath == null || normalizedHdfsPath.isBlank()) {
            return mirrorRoot;
        }
        String relativePath = normalizedHdfsPath.startsWith("/") ? normalizedHdfsPath.substring(1) : normalizedHdfsPath;
        Path mirrorPath = mirrorRoot.resolve(relativePath).normalize();
        if (!mirrorPath.startsWith(mirrorRoot)) {
            throw new IOException("Resolved mirror path escapes mirror directory: " + hdfsPath);
        }
        return mirrorPath;
    }

    private int fileLimit(Integer requestedMaxFiles) {
        if (requestedMaxFiles == null || requestedMaxFiles < 1) {
            return analyticsProps.getDefaultMaxFiles();
        }
        return Math.min(requestedMaxFiles, analyticsProps.getMaxFilesHardLimit());
    }

    private void emitProgress(UUID datasetId, String stage, String message, MutableAnalytics analytics, int totalFiles, List<DashboardProgressEvent.FileProgress> files, boolean complete) {
        emit(datasetId, stage, message, analytics.getScannedFiles(), totalFiles, analytics.getParsedEmails(), analytics.getFailedFiles(), files, complete);
    }

    private void emit(UUID datasetId, String stage, String message, int scannedFiles, int totalFiles, int processedRows, int failedFiles, boolean complete) {
        emit(datasetId, stage, message, scannedFiles, totalFiles, processedRows, failedFiles, List.of(), complete);
    }

    private void emit(UUID datasetId, String stage, String message, int scannedFiles, int totalFiles, int processedRows, int failedFiles, List<DashboardProgressEvent.FileProgress> files, boolean complete) {
        progressService.emit(new DashboardProgressEvent(
                datasetId.toString(),
                stage,
                message,
                scannedFiles,
                totalFiles,
                processedRows,
                failedFiles,
                List.copyOf(files),
                List.of(),
                null,
                complete
        ));
    }

    private void addFileProgress(List<DashboardProgressEvent.FileProgress> files, String filePath, String status, int processedRows, String message) {
        files.removeIf(file -> file.path().equals(filePath));
        files.add(new DashboardProgressEvent.FileProgress(filePath, fileName(filePath), status, processedRows, message));
        while (files.size() > LIVE_FILE_PROGRESS_LIMIT) {
            files.remove(0);
        }
    }

    private String fileName(String filePath) {
        int slashIndex = filePath == null ? -1 : filePath.lastIndexOf('/');
        return slashIndex >= 0 ? filePath.substring(slashIndex + 1) : String.valueOf(filePath);
    }

    private static final class MutableAnalytics {

        private int scannedFiles;
        private int parsedEmails;
        private int failedFiles;
        private final Set<String> uniqueSenders = new HashSet<>();
        private final Set<String> uniqueRecipients = new HashSet<>();
        private final Map<YearMonth, Long> volumeByMonth = new HashMap<>();
        private final Map<Integer, Long> hourlyDistribution = new HashMap<>();
        private final Map<String, Long> senderCounts = new HashMap<>();
        private final Map<String, Long> recipientCounts = new HashMap<>();
        private final Map<String, Long> mailboxOwnerCounts = new HashMap<>();
        private final Map<String, Long> subjectKeywordCounts = new HashMap<>();
        private final Map<EdgeKey, Long> edgeCounts = new HashMap<>();
        private Instant firstEmailAt;
        private Instant lastEmailAt;

        void incrementScannedFiles() {
            scannedFiles++;
        }

        void incrementFailedFiles() {
            failedFiles++;
        }

        int getScannedFiles() {
            return scannedFiles;
        }

        int getParsedEmails() {
            return parsedEmails;
        }

        int getFailedFiles() {
            return failedFiles;
        }

        void accept(EmailRecord record) {
            parsedEmails++;

            String sender = normalize(record.getFrom());
            if (sender != null) {
                uniqueSenders.add(sender);
                senderCounts.merge(sender, 1L, Long::sum);
            }

            Set<String> recipients = new LinkedHashSet<>();
            recipients.addAll(normalizeList(record.getTo()));
            recipients.addAll(normalizeList(record.getCc()));
            recipients.addAll(normalizeList(record.getBcc()));

            for (String recipient : recipients) {
                uniqueRecipients.add(recipient);
                recipientCounts.merge(recipient, 1L, Long::sum);
                if (sender != null) {
                    edgeCounts.merge(new EdgeKey(sender, recipient), 1L, Long::sum);
                }
            }

            if (record.getMailboxOwner() != null && !record.getMailboxOwner().isBlank()) {
                mailboxOwnerCounts.merge(record.getMailboxOwner(), 1L, Long::sum);
            }

            if (record.getSubject() != null && !record.getSubject().isBlank()) {
                for (String keyword : TextAnalyticsUtils.subjectKeywords(record.getSubject())) {
                    subjectKeywordCounts.merge(keyword, 1L, Long::sum);
                }
            }

            if (record.getSentAt() != null) {
                Instant sentAt = record.getSentAt();
                YearMonth yearMonth = YearMonth.from(sentAt.atZone(ZoneOffset.UTC));
                volumeByMonth.merge(yearMonth, 1L, Long::sum);
                int hour = sentAt.atZone(ZoneOffset.UTC).getHour();
                hourlyDistribution.merge(hour, 1L, Long::sum);

                if (firstEmailAt == null || sentAt.isBefore(firstEmailAt)) {
                    firstEmailAt = sentAt;
                }
                if (lastEmailAt == null || sentAt.isAfter(lastEmailAt)) {
                    lastEmailAt = sentAt;
                }
            }
        }

        EmailAnalyticsSnapshot toSnapshot(DatasetRegistration dataset,
                                          int maxFiles,
                                          Instant generatedAt,
                                          AnalyticsProps analyticsProps) {
            EmailAnalyticsSnapshot snapshot = new EmailAnalyticsSnapshot();
            snapshot.setDatasetId(dataset.getId());
            snapshot.setDatasetName(dataset.getName());
            snapshot.setDatasetType(dataset.getDatasetType());
            snapshot.setHdfsPath(dataset.getHdfsPath());
            snapshot.setGeneratedAt(generatedAt);
            snapshot.setMaxFiles(maxFiles);
            snapshot.setOverview(new AnalyticsOverview(
                    scannedFiles,
                    parsedEmails,
                    failedFiles,
                    uniqueSenders.size(),
                    uniqueRecipients.size(),
                    firstEmailAt,
                    lastEmailAt
            ));
            snapshot.setVolumeByMonth(toMonthSeries());
            snapshot.setHourlyDistribution(toHourlySeries());
            snapshot.setTopSenders(toNamedCounts(senderCounts, analyticsProps.getDefaultTopLimit()));
            snapshot.setTopRecipients(toNamedCounts(recipientCounts, analyticsProps.getDefaultTopLimit()));
            snapshot.setTopMailboxOwners(toNamedCounts(mailboxOwnerCounts, analyticsProps.getDefaultTopLimit()));
            snapshot.setTopSubjectKeywords(toNamedCounts(subjectKeywordCounts, analyticsProps.getDefaultTopLimit()));
            snapshot.setCommunicationGraph(toCommunicationEdges(edgeCounts, analyticsProps.getDefaultGraphEdgeLimit()));
            return snapshot;
        }

        private List<TimeSeriesPoint> toMonthSeries() {
            return volumeByMonth.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(entry -> new TimeSeriesPoint(entry.getKey().toString(), entry.getValue()))
                    .toList();
        }

        private List<TimeSeriesPoint> toHourlySeries() {
            List<TimeSeriesPoint> points = new ArrayList<>();
            for (int hour = 0; hour < 24; hour++) {
                String bucket = HOUR_BUCKET_FORMATTER.format(YearMonth.now().atDay(1).atTime(hour, 0));
                points.add(new TimeSeriesPoint(bucket, hourlyDistribution.getOrDefault(hour, 0L)));
            }
            return points;
        }

        private List<NamedCount> toNamedCounts(Map<String, Long> counts, int limit) {
            return counts.entrySet()
                    .stream()
                    .sorted(countComparator())
                    .limit(Math.max(1, limit))
                    .map(entry -> new NamedCount(entry.getKey(), entry.getValue()))
                    .toList();
        }

        private List<CommunicationEdge> toCommunicationEdges(Map<EdgeKey, Long> counts, int limit) {
            return counts.entrySet()
                    .stream()
                    .sorted(Comparator.<Map.Entry<EdgeKey, Long>>comparingLong(Map.Entry::getValue)
                            .reversed()
                            .thenComparing(entry -> entry.getKey().source())
                            .thenComparing(entry -> entry.getKey().target()))
                    .limit(Math.max(1, limit))
                    .map(entry -> new CommunicationEdge(entry.getKey().source(), entry.getKey().target(), entry.getValue()))
                    .toList();
        }

        private Comparator<Map.Entry<String, Long>> countComparator() {
            return Comparator.<Map.Entry<String, Long>>comparingLong(Map.Entry::getValue)
                    .reversed()
                    .thenComparing(Map.Entry::getKey);
        }

        private String normalize(String value) {
            if (value == null || value.isBlank()) {
                return null;
            }
            return value.trim().toLowerCase();
        }

        private List<String> normalizeList(List<String> values) {
            if (values == null || values.isEmpty()) {
                return List.of();
            }
            return values.stream()
                    .map(this::normalize)
                    .filter(value -> value != null && !value.isBlank())
                    .toList();
        }
    }

    private record EdgeKey(String source, String target) {
    }

    private record CachedSnapshot(Instant cachedAt, EmailAnalyticsSnapshot snapshot) {
        boolean isExpired(long ttlMillis) {
            return cachedAt.plusMillis(ttlMillis).isBefore(Instant.now());
        }
    }
}
