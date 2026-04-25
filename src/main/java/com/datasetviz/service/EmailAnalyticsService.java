package com.datasetviz.service;

import com.datasetviz.config.AnalyticsProperties;
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

@Service
public class EmailAnalyticsService {

    private static final DateTimeFormatter HOUR_BUCKET_FORMATTER = DateTimeFormatter.ofPattern("HH");

    private final DatasetRegistryService datasetRegistryService;
    private final HdfsStorageService hdfsStorageService;
    private final EmailArchiveParser emailArchiveParser;
    private final AnalyticsProperties analyticsProperties;
    private final DashboardProgressService dashboardProgressService;
    private final ConcurrentMap<String, CachedSnapshot> cache = new ConcurrentHashMap<>();

    public EmailAnalyticsService(DatasetRegistryService datasetRegistryService,
                                  HdfsStorageService hdfsStorageService,
                                  EmailArchiveParser emailArchiveParser,
                                  AnalyticsProperties analyticsProperties) {
        this(datasetRegistryService, hdfsStorageService, emailArchiveParser, analyticsProperties, new DashboardProgressService());
    }

    @Autowired
    public EmailAnalyticsService(DatasetRegistryService datasetRegistryService,
                                 HdfsStorageService hdfsStorageService,
                                 EmailArchiveParser emailArchiveParser,
                                 AnalyticsProperties analyticsProperties,
                                 DashboardProgressService dashboardProgressService) {
        this.datasetRegistryService = datasetRegistryService;
        this.hdfsStorageService = hdfsStorageService;
        this.emailArchiveParser = emailArchiveParser;
        this.analyticsProperties = analyticsProperties;
        this.dashboardProgressService = dashboardProgressService;
    }

    public EmailAnalyticsSnapshot analyze(UUID datasetId, Integer requestedMaxFiles, boolean refresh) throws IOException {
        DatasetRegistration dataset = datasetRegistryService.getRequired(datasetId);
        if (dataset.getDatasetType() != DatasetType.EMAIL_ARCHIVE) {
            throw new IllegalArgumentException("Current analytics implementation supports EMAIL_ARCHIVE datasets only.");
        }

        int maxFiles = resolveMaxFiles(requestedMaxFiles);
        String cacheKey = datasetId + ":" + maxFiles;
        CachedSnapshot cachedSnapshot = cache.get(cacheKey);
        if (!refresh && cachedSnapshot != null && !cachedSnapshot.isExpired(analyticsProperties.getCacheTtl().toMillis())) {
            AnalyticsOverview overview = cachedSnapshot.snapshot().getOverview();
            publish(datasetId, "cache", "Loaded dashboard analytics from cache.", overview.getScannedFiles(), overview.getScannedFiles(), overview.getParsedEmails(), overview.getFailedFiles(), true);
            return cachedSnapshot.snapshot();
        }

        publish(datasetId, "starting", "Preparing email analytics.", 0, 0, 0, 0, false);
        if (!hdfsStorageService.exists(dataset.getHdfsPath())) {
            throw new IllegalArgumentException("HDFS path does not exist: " + dataset.getHdfsPath());
        }

        MutableAnalytics mutableAnalytics = new MutableAnalytics();
        List<String> filePaths = hdfsStorageService.listFilePaths(dataset.getHdfsPath(), maxFiles);
        publish(datasetId, "listed", "Found " + filePaths.size() + " file(s) to scan.", 0, filePaths.size(), 0, 0, false);
        for (String filePath : filePaths) {
            mutableAnalytics.incrementScannedFiles();
            publishProgress(datasetId, "processing", "Scanning " + fileName(filePath), mutableAnalytics, filePaths.size(), false);
            try (InputStream inputStream = hdfsStorageService.open(filePath)) {
                EmailRecord record = emailArchiveParser.parse(inputStream, filePath).orElse(null);
                if (record == null) {
                    mutableAnalytics.incrementFailedFiles();
                    publishProgress(datasetId, "warning", "Skipped " + fileName(filePath), mutableAnalytics, filePaths.size(), false);
                    continue;
                }
                record.setMailboxOwner(PathUtils.deriveMailboxOwner(dataset.getHdfsPath(), filePath));
                mutableAnalytics.accept(record);
            } catch (Exception exception) {
                mutableAnalytics.incrementFailedFiles();
                publishProgress(datasetId, "warning", "Skipped " + fileName(filePath) + ": " + exception.getMessage(), mutableAnalytics, filePaths.size(), false);
            }
        }

        EmailAnalyticsSnapshot snapshot = mutableAnalytics.toSnapshot(dataset, maxFiles, Instant.now(), analyticsProperties);
        cache.put(cacheKey, new CachedSnapshot(Instant.now(), snapshot));
        publish(datasetId, "complete", "Dashboard analytics ready.", snapshot.getOverview().getScannedFiles(), filePaths.size(), snapshot.getOverview().getParsedEmails(), snapshot.getOverview().getFailedFiles(), true);
        return snapshot;
    }

    private int resolveMaxFiles(Integer requestedMaxFiles) {
        if (requestedMaxFiles == null || requestedMaxFiles < 1) {
            return analyticsProperties.getDefaultMaxFiles();
        }
        return Math.min(requestedMaxFiles, analyticsProperties.getMaxFilesHardLimit());
    }

    private void publishProgress(UUID datasetId, String stage, String message, MutableAnalytics analytics, int totalFiles, boolean complete) {
        publish(datasetId, stage, message, analytics.getScannedFiles(), totalFiles, analytics.getParsedEmails(), analytics.getFailedFiles(), complete);
    }

    private void publish(UUID datasetId, String stage, String message, int scannedFiles, int totalFiles, int processedRows, int failedFiles, boolean complete) {
        dashboardProgressService.publish(new DashboardProgressEvent(
                datasetId.toString(),
                stage,
                message,
                scannedFiles,
                totalFiles,
                processedRows,
                failedFiles,
                List.of(),
                null,
                complete
        ));
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
                                          AnalyticsProperties analyticsProperties) {
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
            snapshot.setTopSenders(toNamedCounts(senderCounts, analyticsProperties.getDefaultTopLimit()));
            snapshot.setTopRecipients(toNamedCounts(recipientCounts, analyticsProperties.getDefaultTopLimit()));
            snapshot.setTopMailboxOwners(toNamedCounts(mailboxOwnerCounts, analyticsProperties.getDefaultTopLimit()));
            snapshot.setTopSubjectKeywords(toNamedCounts(subjectKeywordCounts, analyticsProperties.getDefaultTopLimit()));
            snapshot.setCommunicationGraph(toCommunicationEdges(edgeCounts, analyticsProperties.getDefaultGraphEdgeLimit()));
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
