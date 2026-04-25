package com.datasetviz.service;

import com.datasetviz.config.AnalyticsProperties;
import com.datasetviz.config.DatasetRegistryProperties;
import com.datasetviz.config.HdfsProperties;
import com.datasetviz.dto.DashboardView;
import com.datasetviz.dto.DatasetView;
import com.datasetviz.dto.ImportLocalDirectoryRequest;
import com.datasetviz.dto.RegisterDatasetRequest;
import com.datasetviz.model.AnalyticsOverview;
import com.datasetviz.model.ColumnProfile;
import com.datasetviz.model.CommunicationEdge;
import com.datasetviz.model.CsvAnalyticsOverview;
import com.datasetviz.model.CsvAnalyticsSnapshot;
import com.datasetviz.model.DatasetRegistration;
import com.datasetviz.model.DatasetType;
import com.datasetviz.model.EmailAnalyticsSnapshot;
import com.datasetviz.model.HdfsFileDescriptor;
import com.datasetviz.model.MetricBreakdown;
import com.datasetviz.model.MetricSeries;
import com.datasetviz.model.NamedCount;
import com.datasetviz.model.TimeSeriesPoint;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.web.MockMultipartFile;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BackendServiceTest {

    @Test
    void datasetRegistryServiceRegistersSortsFindsAndPersistsDatasets(@TempDir java.nio.file.Path tempDir) throws Exception {
        DatasetRegistryProperties properties = new DatasetRegistryProperties();
        properties.setPath(tempDir.resolve("registry.json").toFile());
        ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        DatasetRegistryService service = new DatasetRegistryService(objectMapper, properties);
        service.load();

        RegisterDatasetRequest csvRequest = new RegisterDatasetRequest();
        csvRequest.setName("  csv dataset  ");
        csvRequest.setDescription("csv");
        csvRequest.setHdfsPath("/datasets/csv///");
        DatasetRegistration csvDataset = service.register(csvRequest);

        RegisterDatasetRequest defaultRequest = new RegisterDatasetRequest();
        defaultRequest.setName(" mail dataset ");
        defaultRequest.setDescription("mail");
        defaultRequest.setHdfsPath("/datasets/mail/");
        DatasetRegistration emailDataset = service.register(defaultRequest);

        assertThat(csvDataset.getName()).isEqualTo("csv dataset");
        assertThat(csvDataset.getDatasetType()).isEqualTo(DatasetType.GENERIC_FILES);
        assertThat(csvDataset.getHdfsPath()).isEqualTo("/datasets/csv");
        assertThat(emailDataset.getDatasetType()).isEqualTo(DatasetType.GENERIC_FILES);
        assertThat(service.updateDatasetType(csvDataset.getId(), DatasetType.CSV_TEXT).getDatasetType()).isEqualTo(DatasetType.CSV_TEXT);
        assertThat(service.listAll()).containsExactly(emailDataset, csvDataset);
        assertThat(service.getRequired(csvDataset.getId())).isSameAs(csvDataset);
        assertThatThrownBy(() -> service.getRequired(UUID.randomUUID()))
                .isInstanceOf(java.util.NoSuchElementException.class)
                .hasMessageContaining("Dataset not found");

        DatasetRegistryService reloadedService = new DatasetRegistryService(objectMapper, properties);
        reloadedService.load();
        assertThat(reloadedService.listAll()).extracting(DatasetRegistration::getId)
                .containsExactly(emailDataset.getId(), csvDataset.getId());
    }

    @Test
    void datasetImportServiceImportsFilesAndRegistersDataset(@TempDir java.nio.file.Path tempDir) throws Exception {
        java.nio.file.Path nestedDirectory = Files.createDirectories(tempDir.resolve("nested"));
        java.nio.file.Path file = Files.writeString(nestedDirectory.resolve("data.txt"), "hello");

        HdfsStorageService hdfsStorageService = mock(HdfsStorageService.class);
        DatasetRegistryService datasetRegistryService = mock(DatasetRegistryService.class);
        DatasetImportService service = new DatasetImportService(hdfsStorageService, datasetRegistryService, new HdfsProperties());

        UUID datasetId = UUID.randomUUID();
        DatasetRegistration registration = new DatasetRegistration(datasetId, "dataset", "description", DatasetType.CSV_TEXT, "/datasets/import", Instant.now());
        HdfsFileDescriptor descriptor = new HdfsFileDescriptor("/datasets/import/nested/data.txt", "data.txt", false, 5L, Instant.now());
        ImportLocalDirectoryRequest request = new ImportLocalDirectoryRequest();
        request.setDatasetId(datasetId);
        request.setDatasetType(DatasetType.CSV_TEXT);
        request.setLocalDirectory(tempDir.toString());
        request.setTargetSubdirectory("");

        when(datasetRegistryService.updateDatasetType(any(UUID.class), any(DatasetType.class))).thenReturn(registration);
        when(datasetRegistryService.getRequired(datasetId)).thenReturn(registration);
        when(hdfsStorageService.exists("/datasets/import")).thenReturn(true);
        when(hdfsStorageService.listFiles("/datasets/import", true, 500)).thenReturn(List.of(descriptor));

        List<HdfsFileDescriptor> result = service.importLocalDirectory(request);

        assertThat(result).containsExactly(descriptor);
        verify(hdfsStorageService).createDirectories("/datasets/import");
        verify(hdfsStorageService).copyLocalFileToHdfs(file, "/datasets/import/nested/data.txt");
    }

    @Test
    void datasetImportServiceRejectsMissingDirectory() {
        DatasetImportService service = new DatasetImportService(mock(HdfsStorageService.class), mock(DatasetRegistryService.class), new HdfsProperties());
        ImportLocalDirectoryRequest request = new ImportLocalDirectoryRequest();
        request.setLocalDirectory("/path/that/does/not/exist");

        assertThatThrownBy(() -> service.importLocalDirectory(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Local directory does not exist");
    }

    @Test
    void datasetImportServiceImportsIntoTargetSubdirectory(@TempDir java.nio.file.Path tempDir) throws Exception {
        java.nio.file.Path file = Files.writeString(tempDir.resolve("data.txt"), "hello");

        HdfsStorageService hdfsStorageService = mock(HdfsStorageService.class);
        DatasetRegistryService datasetRegistryService = mock(DatasetRegistryService.class);
        DatasetImportService service = new DatasetImportService(hdfsStorageService, datasetRegistryService, new HdfsProperties());
        UUID datasetId = UUID.randomUUID();
        DatasetRegistration registration = new DatasetRegistration(datasetId, "dataset", "", DatasetType.CSV_TEXT, "/datasets/import", Instant.now());

        ImportLocalDirectoryRequest request = new ImportLocalDirectoryRequest();
        request.setDatasetId(datasetId);
        request.setDatasetType(DatasetType.CSV_TEXT);
        request.setLocalDirectory(tempDir.toString());
        request.setTargetSubdirectory("project-a");

        when(datasetRegistryService.updateDatasetType(any(UUID.class), any(DatasetType.class))).thenReturn(registration);
        when(datasetRegistryService.getRequired(datasetId)).thenReturn(registration);
        when(hdfsStorageService.exists("/datasets/import")).thenReturn(true);
        when(hdfsStorageService.listFiles("/datasets/import", true, 500)).thenReturn(List.of());

        List<HdfsFileDescriptor> result = service.importLocalDirectory(request);

        assertThat(result).isEmpty();
        verify(hdfsStorageService).createDirectories("/datasets/import/project-a");
        verify(hdfsStorageService).copyLocalFileToHdfs(file, "/datasets/import/project-a/data.txt");
    }

    @Test
    void datasetImportServiceImportsRemoteFilesAndDeletesDatasetFiles() throws Exception {
        HdfsStorageService hdfsStorageService = mock(HdfsStorageService.class);
        DatasetRegistryService datasetRegistryService = mock(DatasetRegistryService.class);
        DatasetImportService service = new DatasetImportService(hdfsStorageService, datasetRegistryService, new HdfsProperties());
        UUID datasetId = UUID.randomUUID();
        DatasetRegistration registration = new DatasetRegistration(datasetId, "dataset", "", DatasetType.CSV_TEXT, "/datasets/import", Instant.now());
        HdfsFileDescriptor descriptor = new HdfsFileDescriptor("/datasets/import/uploads/data.csv", "data.csv", false, 5L, Instant.now());
        MockMultipartFile file = new MockMultipartFile("files", "data.csv", "text/csv", "a,b".getBytes(StandardCharsets.UTF_8));

        when(datasetRegistryService.updateDatasetType(any(UUID.class), any(DatasetType.class))).thenReturn(registration);
        when(datasetRegistryService.getRequired(datasetId)).thenReturn(registration);
        when(hdfsStorageService.exists("/datasets/import")).thenReturn(true);
        when(hdfsStorageService.listFiles("/datasets/import", true, 500)).thenReturn(List.of(descriptor));
        when(hdfsStorageService.delete("/datasets/import/uploads/data.csv")).thenReturn(true);

        List<HdfsFileDescriptor> result = service.importRemoteFiles(datasetId, DatasetType.CSV_TEXT, new MockMultipartFile[]{file}, "uploads");

        assertThat(result).containsExactly(descriptor);
        verify(hdfsStorageService).createDirectories("/datasets/import/uploads");
        verify(hdfsStorageService).writeToHdfs(any(InputStream.class), eq("/datasets/import/uploads/data.csv"));
        assertThat(service.deleteDatasetFile(datasetId, "/datasets/import/uploads/data.csv")).isTrue();
        assertThatThrownBy(() -> service.deleteDatasetFile(datasetId, "/datasets/import"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("under dataset root");
    }

    @Test
    void datasetImportServiceRejectsDirectoriesOutsideConfiguredLocalPath(@TempDir java.nio.file.Path tempDir) throws Exception {
        java.nio.file.Path allowedRoot = Files.createDirectories(tempDir.resolve("allowed"));
        java.nio.file.Path outsideRoot = Files.createDirectories(tempDir.resolve("outside"));

        HdfsProperties hdfsProperties = new HdfsProperties();
        hdfsProperties.setLocalPath(allowedRoot.toString());
        DatasetRegistryService datasetRegistryService = mock(DatasetRegistryService.class);
        DatasetImportService service = new DatasetImportService(mock(HdfsStorageService.class), datasetRegistryService, hdfsProperties);

        UUID datasetId = UUID.randomUUID();
        when(datasetRegistryService.getRequired(datasetId)).thenReturn(new DatasetRegistration(datasetId, "dataset", "", DatasetType.CSV_TEXT, "/datasets/import", Instant.now()));

        ImportLocalDirectoryRequest request = new ImportLocalDirectoryRequest();
        request.setDatasetId(datasetId);
        request.setLocalDirectory(outsideRoot.toString());

        assertThatThrownBy(() -> service.importLocalDirectory(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be under configured local path");
    }

    @Test
    void datasetImportServiceAllowsDirectoriesUnderConfiguredLocalPath(@TempDir java.nio.file.Path tempDir) throws Exception {
        java.nio.file.Path allowedRoot = Files.createDirectories(tempDir.resolve("allowed"));
        java.nio.file.Path nestedDirectory = Files.createDirectories(allowedRoot.resolve("nested"));
        java.nio.file.Path file = Files.writeString(nestedDirectory.resolve("data.txt"), "hello");

        HdfsStorageService hdfsStorageService = mock(HdfsStorageService.class);
        DatasetRegistryService datasetRegistryService = mock(DatasetRegistryService.class);
        HdfsProperties hdfsProperties = new HdfsProperties();
        hdfsProperties.setLocalPath(allowedRoot.toString());
        DatasetImportService service = new DatasetImportService(hdfsStorageService, datasetRegistryService, hdfsProperties);

        UUID datasetId = UUID.randomUUID();
        DatasetRegistration registration = new DatasetRegistration(datasetId, "dataset", "", DatasetType.CSV_TEXT, "/datasets/import", Instant.now());
        when(datasetRegistryService.updateDatasetType(any(UUID.class), any(DatasetType.class))).thenReturn(registration);
        when(datasetRegistryService.getRequired(datasetId)).thenReturn(registration);
        when(hdfsStorageService.listFiles("/datasets/import", true, 500)).thenReturn(List.of());

        ImportLocalDirectoryRequest request = new ImportLocalDirectoryRequest();
        request.setDatasetId(datasetId);
        request.setDatasetType(DatasetType.CSV_TEXT);
        request.setLocalDirectory(nestedDirectory.toString());
        request.setTargetSubdirectory("");

        List<HdfsFileDescriptor> result = service.importLocalDirectory(request);

        assertThat(result).isEmpty();
        verify(hdfsStorageService).copyLocalFileToHdfs(file, "/datasets/import/data.txt");
    }

    @Test
    void dashboardViewServiceBuildsEmailDashboardAndDatasetView() {
        DashboardViewService service = new DashboardViewService();

        EmailAnalyticsSnapshot snapshot = new EmailAnalyticsSnapshot();
        snapshot.setDatasetId(UUID.randomUUID());
        snapshot.setDatasetName("mail");
        snapshot.setDatasetType(DatasetType.EMAIL_ARCHIVE);
        snapshot.setHdfsPath("/datasets/mail");
        snapshot.setGeneratedAt(Instant.parse("2024-01-01T00:00:00Z"));
        snapshot.setMaxFiles(25);
        snapshot.setOverview(new AnalyticsOverview(10, 8, 2, 3, 4, Instant.parse("2024-01-02T00:00:00Z"), Instant.parse("2024-01-03T00:00:00Z")));
        snapshot.setVolumeByMonth(List.of(new TimeSeriesPoint("2024-01", 8)));
        snapshot.setHourlyDistribution(List.of(new TimeSeriesPoint("09", 4)));
        snapshot.setTopSenders(List.of(new NamedCount("alice@example.com", 5)));
        snapshot.setTopRecipients(List.of(new NamedCount("bob@example.com", 4)));
        snapshot.setTopSubjectKeywords(List.of(new NamedCount("incident", 2)));
        snapshot.setCommunicationGraph(List.of(new CommunicationEdge("alice@example.com", "bob@example.com", 3)));

        DashboardView dashboard = service.toDashboardView(snapshot);

        assertThat(dashboard.datasetName()).isEqualTo("mail");
        assertThat(dashboard.summaryItems()).extracting(DashboardView.SummaryItem::label).contains("Dataset", "Parsed emails");
        assertThat(dashboard.charts()).extracting(DashboardView.Chart::id)
                .containsExactly("volume-by-month", "top-senders", "top-recipients", "hourly-distribution");
        assertThat(dashboard.listPanel().items()).containsExactly("incident (2)");
        assertThat(dashboard.tablePanel().rows()).hasSize(1);
        assertThat(dashboard.tablePanel().rows().get(0).cells()).containsExactly("alice@example.com", "bob@example.com", "3");

        DatasetRegistration registration = new DatasetRegistration(UUID.randomUUID(), "mail", "desc", DatasetType.EMAIL_ARCHIVE, "/datasets/mail", Instant.parse("2024-01-04T00:00:00Z"));
        DatasetView dataset = service.toDatasetView(registration);

        assertThat(dataset.id()).isEqualTo(registration.getId().toString());
        assertThat(dataset.registeredAt()).isEqualTo("2024-01-04T00:00:00Z");
    }

    @Test
    void dashboardViewServiceBuildsCsvDashboardAndRejectsUnsupportedSnapshots() {
        DashboardViewService service = new DashboardViewService();

        CsvAnalyticsOverview overview = new CsvAnalyticsOverview();
        overview.setScannedFiles(5);
        overview.setProcessedRows(100);
        overview.setFailedFiles(1);
        overview.setDistinctLocations(2);
        overview.setDetectedMetrics(2);
        overview.setDateColumn("ObservationDate");
        overview.setLocationColumn("Province");
        overview.setMetricColumns(List.of("Confirmed", "Recovered"));
        overview.setFirstObservedAt(Instant.parse("2024-02-01T00:00:00Z"));
        overview.setLastObservedAt(Instant.parse("2024-02-02T00:00:00Z"));

        CsvAnalyticsSnapshot snapshot = new CsvAnalyticsSnapshot();
        snapshot.setDatasetId(UUID.randomUUID());
        snapshot.setDatasetName("covid");
        snapshot.setDatasetType(DatasetType.CSV_TEXT);
        snapshot.setHdfsPath("/datasets/covid");
        snapshot.setGeneratedAt(Instant.parse("2024-02-03T00:00:00Z"));
        snapshot.setMaxFiles(10);
        snapshot.setOverview(overview);
        snapshot.setRowsByDate(List.of(new TimeSeriesPoint("2024-02-01", 40), new TimeSeriesPoint("2024-02-02", 60)));
        snapshot.setMetricTotals(List.of(new NamedCount("Confirmed", 70), new NamedCount("Recovered", 30)));
        snapshot.setMetricTimeSeries(List.of(
                new MetricSeries("Confirmed", List.of(new TimeSeriesPoint("2024-02-01", 40), new TimeSeriesPoint("2024-02-02", 70))),
                new MetricSeries("Recovered", List.of(new TimeSeriesPoint("2024-02-01", 10), new TimeSeriesPoint("2024-02-02", 30)))
        ));
        snapshot.setTopLocationsByMetric(List.of(
                new MetricBreakdown("Confirmed", List.of(new NamedCount("Calgary", 50), new NamedCount("Edmonton", 20))),
                new MetricBreakdown("Recovered", List.of(new NamedCount("Calgary", 25), new NamedCount("Edmonton", 5)))
        ));
        snapshot.setColumnProfiles(List.of(new ColumnProfile("Confirmed", "NUMBER", List.of("40", "70"))));

        DashboardView dashboard = service.toDashboardView(snapshot);

        assertThat(dashboard.datasetType()).isEqualTo(DatasetType.CSV_TEXT);
        assertThat(dashboard.charts()).hasSize(4);
        assertThat(dashboard.charts().get(3).series()).extracting(DashboardView.Series::name).containsExactly("Confirmed", "Recovered");
        assertThat(dashboard.listPanel().items()).contains("Metric: Confirmed", "Recovered: 30");
        assertThat(dashboard.columnProfiles()).singleElement()
                .extracting(DashboardView.ColumnPreview::name, DashboardView.ColumnPreview::type)
                .containsExactly("Confirmed", "NUMBER");
        assertThat(dashboard.tablePanel().columns()).containsExactly("Location", "Confirmed", "Recovered");
        assertThat(dashboard.tablePanel().rows().get(0).cells().get(0)).isEqualTo("Calgary");

        assertThatThrownBy(() -> service.toDashboardView(new Object()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("EMAIL_ARCHIVE and CSV_TEXT");
    }

    @Test
    void hdfsStorageServiceDelegatesToFileSystem(@TempDir java.nio.file.Path tempDir) throws Exception {
        FileSystem fileSystem = mock(FileSystem.class);
        HdfsStorageService service = new HdfsStorageService(fileSystem);
        FSDataInputStream inputStream = mock(FSDataInputStream.class);

        when(fileSystem.exists(new Path("/datasets/data"))).thenReturn(true);
        when(fileSystem.open(new Path("/datasets/data/file.txt"))).thenReturn(inputStream);
        FSDataOutputStream outputStream = mock(FSDataOutputStream.class);
        when(fileSystem.create(new Path("/datasets/data/upload.txt"), true)).thenReturn(outputStream);
        when(fileSystem.delete(new Path("/datasets/data/upload.txt"), true)).thenReturn(true);

        assertThat(service.exists("/datasets/data")).isTrue();
        service.createDirectories("/datasets/data");
        verify(fileSystem).mkdirs(new Path("/datasets/data"));

        java.nio.file.Path localFile = Files.writeString(tempDir.resolve("local.txt"), "content");
        service.copyLocalFileToHdfs(localFile, "/datasets/data/local.txt");
        verify(fileSystem, times(2)).mkdirs(new Path("/datasets/data"));

        service.copyLocalFileToHdfs(localFile, "/");
        verify(fileSystem, never()).mkdirs(new Path("/"));
        verify(fileSystem, times(2)).copyFromLocalFile(eq(false), eq(true), any(Path.class), any(Path.class));

        service.writeToHdfs(new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8)), "/datasets/data/upload.txt");
        verify(fileSystem, times(3)).mkdirs(new Path("/datasets/data"));
        assertThat(service.delete("/datasets/data/upload.txt")).isTrue();

        assertThat(service.open("/datasets/data/file.txt")).isSameAs(inputStream);
    }

    @Test
    void hdfsStorageServiceListsFilesAndPaths() throws Exception {
        FileSystem fileSystem = mock(FileSystem.class);
        HdfsStorageService service = new HdfsStorageService(fileSystem);

        LocatedFileStatus recursiveOne = mock(LocatedFileStatus.class);
        when(recursiveOne.getPath()).thenReturn(new Path("/datasets/data/one.txt"));
        when(recursiveOne.isDirectory()).thenReturn(false);
        when(recursiveOne.getLen()).thenReturn(10L);
        when(recursiveOne.getModificationTime()).thenReturn(1000L);

        LocatedFileStatus recursiveTwo = mock(LocatedFileStatus.class);
        when(recursiveTwo.getPath()).thenReturn(new Path("/datasets/data/two.txt"));
        when(recursiveTwo.isDirectory()).thenReturn(false);
        when(recursiveTwo.getLen()).thenReturn(11L);
        when(recursiveTwo.getModificationTime()).thenReturn(2000L);

        @SuppressWarnings("unchecked")
        RemoteIterator<LocatedFileStatus> recursiveIterator = mock(RemoteIterator.class);
        when(recursiveIterator.hasNext()).thenReturn(true, true, false);
        when(recursiveIterator.next()).thenReturn(recursiveOne, recursiveTwo);
        when(fileSystem.listFiles(new Path("/datasets/data"), true)).thenReturn(recursiveIterator);

        List<HdfsFileDescriptor> recursiveFiles = service.listFiles("/datasets/data", true, 5);
        assertThat(recursiveFiles).hasSize(2);
        assertThat(recursiveFiles.get(0).getName()).isEqualTo("one.txt");

        FileStatus directOne = mock(FileStatus.class);
        when(directOne.getPath()).thenReturn(new Path("/datasets/data/direct.txt"));
        when(directOne.isDirectory()).thenReturn(false);
        when(directOne.getLen()).thenReturn(12L);
        when(directOne.getModificationTime()).thenReturn(3000L);

        FileStatus directTwo = mock(FileStatus.class);
        when(directTwo.getPath()).thenReturn(new Path("/datasets/data/other.txt"));
        when(directTwo.isDirectory()).thenReturn(true);
        when(directTwo.getLen()).thenReturn(0L);
        when(directTwo.getModificationTime()).thenReturn(4000L);

        when(fileSystem.listStatus(new Path("/datasets/data"))).thenReturn(new FileStatus[]{directOne, directTwo});
        List<HdfsFileDescriptor> directFiles = service.listFiles("/datasets/data", false, 1);
        assertThat(directFiles).hasSize(1);
        assertThat(directFiles.get(0).getName()).isEqualTo("direct.txt");

        @SuppressWarnings("unchecked")
        RemoteIterator<LocatedFileStatus> pathIterator = mock(RemoteIterator.class);
        when(pathIterator.hasNext()).thenReturn(true, false);
        when(pathIterator.next()).thenReturn(recursiveOne);
        when(fileSystem.listFiles(new Path("/datasets/data/paths"), true)).thenReturn(pathIterator);

        assertThat(service.listFilePaths("/datasets/data/paths", 0)).containsExactly("/datasets/data/one.txt");
    }

    @Test
    void datasetAnalyticsServiceRoutesByDatasetType() throws Exception {
        DatasetRegistryService datasetRegistryService = mock(DatasetRegistryService.class);
        EmailAnalyticsService emailAnalyticsService = mock(EmailAnalyticsService.class);
        CsvAnalyticsService csvAnalyticsService = mock(CsvAnalyticsService.class);
        DatasetAnalyticsService service = new DatasetAnalyticsService(datasetRegistryService, emailAnalyticsService, csvAnalyticsService);

        UUID emailId = UUID.randomUUID();
        UUID csvId = UUID.randomUUID();
        when(datasetRegistryService.getRequired(emailId)).thenReturn(new DatasetRegistration(emailId, "mail", "", DatasetType.EMAIL_ARCHIVE, "/mail", Instant.now()));
        when(datasetRegistryService.getRequired(csvId)).thenReturn(new DatasetRegistration(csvId, "csv", "", DatasetType.CSV_TEXT, "/csv", Instant.now()));
        when(emailAnalyticsService.analyze(emailId, 1, true)).thenReturn(new EmailAnalyticsSnapshot());
        when(csvAnalyticsService.analyze(csvId, 2, null, null, false)).thenReturn(new com.datasetviz.model.CsvAnalyticsSnapshot());

        assertThat(service.analyze(emailId, 1, true)).isInstanceOf(EmailAnalyticsSnapshot.class);
        assertThat(service.analyze(csvId, 2, false)).isInstanceOf(com.datasetviz.model.CsvAnalyticsSnapshot.class);

        UUID genericId = UUID.randomUUID();
        when(datasetRegistryService.getRequired(genericId)).thenReturn(new DatasetRegistration(genericId, "generic", "", DatasetType.GENERIC_FILES, "/generic", Instant.now()));
        assertThatThrownBy(() -> service.analyze(genericId, null, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("EMAIL_ARCHIVE and CSV_TEXT");
    }

    @Test
    void emailAnalyticsServiceAnalyzesCachesAndRejectsInvalidInputs() throws Exception {
        DatasetRegistryService datasetRegistryService = mock(DatasetRegistryService.class);
        HdfsStorageService hdfsStorageService = mock(HdfsStorageService.class);
        AnalyticsProperties analyticsProperties = new AnalyticsProperties();
        analyticsProperties.setCacheTtl(Duration.ofMinutes(5));
        analyticsProperties.setDefaultMaxFiles(5);
        analyticsProperties.setMaxFilesHardLimit(1);
        EmailAnalyticsService service = new EmailAnalyticsService(datasetRegistryService, hdfsStorageService, new EmailArchiveParser(), analyticsProperties);

        UUID datasetId = UUID.randomUUID();
        DatasetRegistration dataset = new DatasetRegistration(datasetId, "mail", "", DatasetType.EMAIL_ARCHIVE, "/datasets/mail", Instant.now());
        when(datasetRegistryService.getRequired(datasetId)).thenReturn(dataset);
        when(hdfsStorageService.exists("/datasets/mail")).thenReturn(true);
        when(hdfsStorageService.listFilePaths("/datasets/mail", 1)).thenReturn(List.of("/datasets/mail/john/1", "/datasets/mail/john/2", "/datasets/mail/john/3"));
        when(hdfsStorageService.open("/datasets/mail/john/1")).thenReturn(new ByteArrayInputStream((
                "Message-ID: <1@test>\n" +
                "Date: Mon, 14 May 2001 16:39:00 -0700 (PDT)\n" +
                "From: john@example.com\n" +
                "To: jane@example.com, bob@example.com\n" +
                "Subject: Trading update\n\n" +
                "Body text."
        ).getBytes(StandardCharsets.UTF_8)));
        when(hdfsStorageService.open("/datasets/mail/john/2")).thenReturn(new ByteArrayInputStream("   ".getBytes(StandardCharsets.UTF_8)));
        when(hdfsStorageService.open("/datasets/mail/john/3")).thenThrow(new IOException("boom"));

        EmailAnalyticsSnapshot first = service.analyze(datasetId, 10, false);
        assertThat(first.getDatasetType()).isEqualTo(DatasetType.EMAIL_ARCHIVE);
        assertThat(first.getMaxFiles()).isEqualTo(1);
        assertThat(first.getOverview().getScannedFiles()).isEqualTo(3);
        assertThat(first.getOverview().getParsedEmails()).isEqualTo(1);
        assertThat(first.getOverview().getFailedFiles()).isEqualTo(2);
        assertThat(first.getTopSenders()).extracting("name").contains("john@example.com");
        assertThat(first.getTopRecipients()).extracting("name").contains("jane@example.com", "bob@example.com");
        assertThat(first.getTopMailboxOwners()).extracting("name").contains("john");
        assertThat(first.getTopSubjectKeywords()).extracting("name").contains("trading", "update");
        assertThat(first.getCommunicationGraph()).hasSize(2);

        EmailAnalyticsSnapshot second = service.analyze(datasetId, 10, false);
        assertThat(second).isSameAs(first);
        verify(hdfsStorageService, times(1)).listFilePaths("/datasets/mail", 1);

        UUID unsupportedId = UUID.randomUUID();
        when(datasetRegistryService.getRequired(unsupportedId)).thenReturn(new DatasetRegistration(unsupportedId, "csv", "", DatasetType.CSV_TEXT, "/csv", Instant.now()));
        assertThatThrownBy(() -> service.analyze(unsupportedId, null, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("EMAIL_ARCHIVE datasets only");

        UUID missingId = UUID.randomUUID();
        when(datasetRegistryService.getRequired(missingId)).thenReturn(new DatasetRegistration(missingId, "mail", "", DatasetType.EMAIL_ARCHIVE, "/missing", Instant.now()));
        when(hdfsStorageService.exists("/missing")).thenReturn(false);
        assertThatThrownBy(() -> service.analyze(missingId, null, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("HDFS path does not exist");
    }
}
