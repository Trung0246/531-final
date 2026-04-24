package com.datasetviz.model;

import com.datasetviz.config.HdfsProperties;
import com.datasetviz.dto.ImportLocalDirectoryRequest;
import com.datasetviz.dto.RegisterDatasetRequest;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ModelAndDtoTest {

    @Test
    void emailModelsRoundTrip() {
        Instant instant = Instant.now();
        assertThat(new CommunicationEdge()).isNotNull();
        assertThat(new NamedCount()).isNotNull();
        assertThat(new TimeSeriesPoint()).isNotNull();
        assertThat(new DatasetRegistration()).isNotNull();
        assertThat(new HdfsFileDescriptor()).isNotNull();

        AnalyticsOverview overview = new AnalyticsOverview(1, 2, 3, 4, 5, instant, instant.plusSeconds(1));
        assertThat(overview.getScannedFiles()).isEqualTo(1);
        assertThat(overview.getParsedEmails()).isEqualTo(2);
        assertThat(overview.getFailedFiles()).isEqualTo(3);
        assertThat(overview.getUniqueSenders()).isEqualTo(4);
        assertThat(overview.getUniqueRecipients()).isEqualTo(5);
        assertThat(overview.getFirstEmailAt()).isEqualTo(instant);
        assertThat(overview.getLastEmailAt()).isEqualTo(instant.plusSeconds(1));

        overview.setScannedFiles(6);
        overview.setParsedEmails(7);
        overview.setFailedFiles(8);
        overview.setUniqueSenders(9);
        overview.setUniqueRecipients(10);
        overview.setFirstEmailAt(instant.plusSeconds(2));
        overview.setLastEmailAt(instant.plusSeconds(3));
        assertThat(overview.getScannedFiles()).isEqualTo(6);
        assertThat(overview.getParsedEmails()).isEqualTo(7);
        assertThat(overview.getFailedFiles()).isEqualTo(8);
        assertThat(overview.getUniqueSenders()).isEqualTo(9);
        assertThat(overview.getUniqueRecipients()).isEqualTo(10);

        CommunicationEdge edge = new CommunicationEdge("a", "b", 11);
        edge.setSource("c");
        edge.setTarget("d");
        edge.setCount(12);
        assertThat(edge.getSource()).isEqualTo("c");
        assertThat(edge.getTarget()).isEqualTo("d");
        assertThat(edge.getCount()).isEqualTo(12);

        DatasetRegistration datasetRegistration = new DatasetRegistration(UUID.randomUUID(), "dataset", "desc", DatasetType.CSV_TEXT, "/path", instant);
        UUID replacementId = UUID.randomUUID();
        datasetRegistration.setId(replacementId);
        datasetRegistration.setName("updated");
        datasetRegistration.setDescription("updated-desc");
        datasetRegistration.setDatasetType(DatasetType.EMAIL_ARCHIVE);
        datasetRegistration.setHdfsPath("/updated");
        datasetRegistration.setRegisteredAt(instant.plusSeconds(4));
        assertThat(datasetRegistration.getId()).isEqualTo(replacementId);
        assertThat(datasetRegistration.getName()).isEqualTo("updated");
        assertThat(datasetRegistration.getDescription()).isEqualTo("updated-desc");
        assertThat(datasetRegistration.getDatasetType()).isEqualTo(DatasetType.EMAIL_ARCHIVE);
        assertThat(datasetRegistration.getHdfsPath()).isEqualTo("/updated");

        EmailRecord emailRecord = new EmailRecord();
        emailRecord.setPath("/mail/1");
        emailRecord.setMessageId("id");
        emailRecord.setSentAt(instant);
        emailRecord.setFrom("from@example.com");
        emailRecord.setTo(List.of("to@example.com"));
        emailRecord.setCc(List.of("cc@example.com"));
        emailRecord.setBcc(List.of("bcc@example.com"));
        emailRecord.setSubject("subject");
        emailRecord.setBodyPreview("preview");
        emailRecord.setMailboxOwner("owner");
        assertThat(emailRecord.getPath()).isEqualTo("/mail/1");
        assertThat(emailRecord.getMessageId()).isEqualTo("id");
        assertThat(emailRecord.getSentAt()).isEqualTo(instant);
        assertThat(emailRecord.getFrom()).isEqualTo("from@example.com");
        assertThat(emailRecord.getTo()).containsExactly("to@example.com");
        assertThat(emailRecord.getCc()).containsExactly("cc@example.com");
        assertThat(emailRecord.getBcc()).containsExactly("bcc@example.com");
        assertThat(emailRecord.getSubject()).isEqualTo("subject");
        assertThat(emailRecord.getBodyPreview()).isEqualTo("preview");
        assertThat(emailRecord.getMailboxOwner()).isEqualTo("owner");

        HdfsFileDescriptor descriptor = new HdfsFileDescriptor("/path", "name", true, 13L, instant);
        descriptor.setPath("/other");
        descriptor.setName("other");
        descriptor.setDirectory(false);
        descriptor.setLength(14L);
        descriptor.setModificationTime(instant.plusSeconds(5));
        assertThat(descriptor.getPath()).isEqualTo("/other");
        assertThat(descriptor.getName()).isEqualTo("other");
        assertThat(descriptor.isDirectory()).isFalse();
        assertThat(descriptor.getLength()).isEqualTo(14L);
        assertThat(descriptor.getModificationTime()).isEqualTo(instant.plusSeconds(5));

        NamedCount namedCount = new NamedCount("metric", 15L);
        namedCount.setName("updated-metric");
        namedCount.setCount(16L);
        assertThat(namedCount.getName()).isEqualTo("updated-metric");
        assertThat(namedCount.getCount()).isEqualTo(16L);

        TimeSeriesPoint point = new TimeSeriesPoint("2024-01", 17L);
        point.setBucket("2024-02");
        point.setCount(18L);
        assertThat(point.getBucket()).isEqualTo("2024-02");
        assertThat(point.getCount()).isEqualTo(18L);

        EmailAnalyticsSnapshot snapshot = new EmailAnalyticsSnapshot();
        snapshot.setDatasetId(UUID.randomUUID());
        snapshot.setDatasetName("snapshot");
        snapshot.setDatasetType(DatasetType.EMAIL_ARCHIVE);
        snapshot.setHdfsPath("/snapshot");
        snapshot.setGeneratedAt(instant);
        snapshot.setMaxFiles(19);
        snapshot.setOverview(overview);
        snapshot.setVolumeByMonth(List.of(point));
        snapshot.setHourlyDistribution(List.of(point));
        snapshot.setTopSenders(List.of(namedCount));
        snapshot.setTopRecipients(List.of(namedCount));
        snapshot.setTopMailboxOwners(List.of(namedCount));
        snapshot.setTopSubjectKeywords(List.of(namedCount));
        snapshot.setCommunicationGraph(List.of(edge));
        assertThat(snapshot.getDatasetId()).isNotNull();
        assertThat(snapshot.getDatasetName()).isEqualTo("snapshot");
        assertThat(snapshot.getDatasetType()).isEqualTo(DatasetType.EMAIL_ARCHIVE);
        assertThat(snapshot.getHdfsPath()).isEqualTo("/snapshot");
        assertThat(snapshot.getGeneratedAt()).isEqualTo(instant);
        assertThat(snapshot.getVolumeByMonth()).containsExactly(point);
        assertThat(snapshot.getCommunicationGraph()).containsExactly(edge);
    }

    @Test
    void csvModelsAndDtosRoundTrip() {
        Instant instant = Instant.now();
        assertThat(new MetricSeries()).isNotNull();
        assertThat(new MetricBreakdown()).isNotNull();
        TimeSeriesPoint point = new TimeSeriesPoint("2020-01-01", 2L);
        NamedCount namedCount = new NamedCount("Confirmed", 3L);

        CsvAnalyticsOverview overview = new CsvAnalyticsOverview();
        overview.setScannedFiles(1);
        overview.setProcessedRows(2);
        overview.setFailedFiles(3);
        overview.setDistinctLocations(4);
        overview.setDetectedMetrics(5);
        overview.setDateColumn("ObservationDate");
        overview.setLocationColumn("Country/Region");
        overview.setMetricColumns(List.of("Confirmed", "Deaths"));
        overview.setFirstObservedAt(instant);
        overview.setLastObservedAt(instant.plusSeconds(1));
        assertThat(overview.getScannedFiles()).isEqualTo(1);
        assertThat(overview.getProcessedRows()).isEqualTo(2);
        assertThat(overview.getFailedFiles()).isEqualTo(3);
        assertThat(overview.getDistinctLocations()).isEqualTo(4);
        assertThat(overview.getDetectedMetrics()).isEqualTo(5);
        assertThat(overview.getDateColumn()).isEqualTo("ObservationDate");
        assertThat(overview.getLocationColumn()).isEqualTo("Country/Region");
        assertThat(overview.getMetricColumns()).containsExactly("Confirmed", "Deaths");
        assertThat(overview.getFirstObservedAt()).isEqualTo(instant);
        assertThat(overview.getLastObservedAt()).isEqualTo(instant.plusSeconds(1));

        MetricSeries metricSeries = new MetricSeries("Confirmed", List.of(point));
        metricSeries.setName("Deaths");
        metricSeries.setPoints(List.of(point));
        assertThat(metricSeries.getName()).isEqualTo("Deaths");
        assertThat(metricSeries.getPoints()).containsExactly(point);

        MetricBreakdown metricBreakdown = new MetricBreakdown("Confirmed", List.of(namedCount));
        metricBreakdown.setName("Recovered");
        metricBreakdown.setItems(List.of(namedCount));
        assertThat(metricBreakdown.getName()).isEqualTo("Recovered");
        assertThat(metricBreakdown.getItems()).containsExactly(namedCount);

        CsvAnalyticsSnapshot snapshot = new CsvAnalyticsSnapshot();
        snapshot.setDatasetId(UUID.randomUUID());
        snapshot.setDatasetName("csv");
        snapshot.setDatasetType(DatasetType.CSV_TEXT);
        snapshot.setHdfsPath("/csv");
        snapshot.setGeneratedAt(instant);
        snapshot.setMaxFiles(6);
        snapshot.setOverview(overview);
        snapshot.setRowsByDate(List.of(point));
        snapshot.setMetricTimeSeries(List.of(metricSeries));
        snapshot.setMetricTotals(List.of(namedCount));
        snapshot.setTopLocationsByMetric(List.of(metricBreakdown));
        assertThat(snapshot.getDatasetId()).isNotNull();
        assertThat(snapshot.getDatasetName()).isEqualTo("csv");
        assertThat(snapshot.getDatasetType()).isEqualTo(DatasetType.CSV_TEXT);
        assertThat(snapshot.getHdfsPath()).isEqualTo("/csv");
        assertThat(snapshot.getGeneratedAt()).isEqualTo(instant);
        assertThat(snapshot.getMaxFiles()).isEqualTo(6);
        assertThat(snapshot.getRowsByDate()).containsExactly(point);
        assertThat(snapshot.getMetricTimeSeries()).containsExactly(metricSeries);
        assertThat(snapshot.getMetricTotals()).containsExactly(namedCount);
        assertThat(snapshot.getTopLocationsByMetric()).containsExactly(metricBreakdown);

        RegisterDatasetRequest registerRequest = new RegisterDatasetRequest();
        assertThat(registerRequest.getDatasetType()).isEqualTo(DatasetType.EMAIL_ARCHIVE);
        registerRequest.setName("name");
        registerRequest.setDescription("description");
        registerRequest.setDatasetType(DatasetType.CSV_TEXT);
        registerRequest.setHdfsPath("/datasets/csv");
        assertThat(registerRequest.getName()).isEqualTo("name");
        assertThat(registerRequest.getDescription()).isEqualTo("description");
        assertThat(registerRequest.getDatasetType()).isEqualTo(DatasetType.CSV_TEXT);
        assertThat(registerRequest.getHdfsPath()).isEqualTo("/datasets/csv");

        ImportLocalDirectoryRequest importRequest = new ImportLocalDirectoryRequest();
        UUID importDatasetId = UUID.randomUUID();
        importRequest.setDatasetId(importDatasetId);
        importRequest.setLocalDirectory("/tmp/data");
        importRequest.setTargetSubdirectory("incoming");
        assertThat(importRequest.getDatasetId()).isEqualTo(importDatasetId);
        assertThat(importRequest.getLocalDirectory()).isEqualTo("/tmp/data");
        assertThat(importRequest.getTargetSubdirectory()).isEqualTo("incoming");

        HdfsProperties hdfsProperties = new HdfsProperties();
        hdfsProperties.setUri("hdfs://custom:9000");
        hdfsProperties.setUser("user");
        hdfsProperties.setConfiguration(Map.of("k", "v"));
        assertThat(hdfsProperties.getUri()).isEqualTo("hdfs://custom:9000");
        assertThat(hdfsProperties.getUser()).isEqualTo("user");
        assertThat(hdfsProperties.getConfiguration()).containsEntry("k", "v");

        assertThat(DatasetType.values()).containsExactly(DatasetType.EMAIL_ARCHIVE, DatasetType.CSV_TEXT, DatasetType.GENERIC_FILES);
    }
}
