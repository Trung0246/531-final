package com.datasetviz.service;

import com.datasetviz.config.AnalyticsProps;
import com.datasetviz.model.CsvAnalyticsSnapshot;
import com.datasetviz.model.DatasetRegistration;
import com.datasetviz.model.DatasetType;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CsvServiceTest {

    @Test
    void rejectsUnsupportedDatasetTypeAndMissingPath() throws Exception {
        RegistryService registryService = mock(RegistryService.class);
        HdfsStore hdfsStore = mock(HdfsStore.class);
        CsvService csvService = new CsvService(registryService, hdfsStore, new AnalyticsProps());

        UUID unsupportedId = UUID.randomUUID();
        when(registryService.getRequired(unsupportedId)).thenReturn(new DatasetRegistration(
                unsupportedId,
                "mail",
                "desc",
                DatasetType.EMAIL_ARCHIVE,
                "/datasets/mail",
                Instant.now()
        ));

        assertThatThrownBy(() -> csvService.analyze(unsupportedId, null, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CSV_TEXT datasets only");

        UUID missingId = UUID.randomUUID();
        when(registryService.getRequired(missingId)).thenReturn(new DatasetRegistration(
                missingId,
                "csv",
                "desc",
                DatasetType.CSV_TEXT,
                "/datasets/missing",
                Instant.now()
        ));
        when(hdfsStore.exists("/datasets/missing")).thenReturn(false);

        assertThatThrownBy(() -> csvService.analyze(missingId, null, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("HDFS path does not exist");
    }

    @Test
    void analyzesCoronaStyleCsvDataset() throws Exception {
        RegistryService registryService = mock(RegistryService.class);
        HdfsStore hdfsStore = mock(HdfsStore.class);
        AnalyticsProps analyticsProps = new AnalyticsProps();
        analyticsProps.setCacheTtl(Duration.ofMinutes(5));
        CsvService csvService = new CsvService(
                registryService,
                hdfsStore,
                analyticsProps
        );

        UUID datasetId = UUID.randomUUID();
        DatasetRegistration dataset = new DatasetRegistration(
                datasetId,
                "covid-report",
                "Corona virus daily report",
                DatasetType.CSV_TEXT,
                "/datasets/covid-report",
                Instant.now()
        );

        String csv = "ObservationDate,Country/Region,Confirmed,Deaths,Recovered\n"
                + "1/22/2020,US,1,0,0\n"
                + "1/22/2020,Canada,0,0,0\n"
                + "1/23/2020,US,2,0,0\n"
                + "1/23/2020,Canada,1,0,0\n";

        when(registryService.getRequired(datasetId)).thenReturn(dataset);
        when(hdfsStore.exists("/datasets/covid-report")).thenReturn(true);
        when(hdfsStore.listFilePaths("/datasets/covid-report", analyticsProps.getDefaultMaxFiles()))
                .thenReturn(List.of("/datasets/covid-report/daily.csv"));
        when(hdfsStore.open("/datasets/covid-report/daily.csv"))
                .thenReturn(new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)));

        CsvAnalyticsSnapshot snapshot = csvService.analyze(datasetId, null, true);

        assertThat(snapshot.getDatasetType()).isEqualTo(DatasetType.CSV_TEXT);
        assertThat(snapshot.getOverview().getProcessedRows()).isEqualTo(4);
        assertThat(snapshot.getOverview().getDistinctLocations()).isEqualTo(2);
        assertThat(snapshot.getOverview().getDateColumn()).isEqualTo("ObservationDate");
        assertThat(snapshot.getOverview().getLocationColumn()).isEqualTo("Country/Region");
        assertThat(snapshot.getMetricTotals())
                .extracting(item -> item.getName() + ":" + item.getCount())
                .contains("Confirmed:3", "Deaths:0", "Recovered:0");
        assertThat(snapshot.getRowsByDate())
                .extracting(point -> point.getBucket() + ":" + point.getCount())
                .containsExactly("2020-01-22:2", "2020-01-23:2");
        assertThat(snapshot.getTopLocationsByMetric())
                .first()
                .extracting(breakdown -> breakdown.getItems())
                .asList()
                .first()
                .extracting("name", "count")
                .containsExactly("US", 2L);

        CsvAnalyticsSnapshot cachedSnapshot = csvService.analyze(datasetId, null, false);
        assertThat(cachedSnapshot).isSameAs(snapshot);
        verify(hdfsStore, times(1)).open("/datasets/covid-report/daily.csv");
    }

    @Test
    void analyzesCsvWithoutDateColumnUsingFallbackMetricTotals() throws Exception {
        RegistryService registryService = mock(RegistryService.class);
        HdfsStore hdfsStore = mock(HdfsStore.class);
        CsvService csvService = new CsvService(registryService, hdfsStore, new AnalyticsProps());

        UUID datasetId = UUID.randomUUID();
        DatasetRegistration dataset = new DatasetRegistration(datasetId, "values", "desc", DatasetType.CSV_TEXT, "/datasets/values", Instant.now());
        String csv = "Location,Value\nUS,5\nCanada,3\n";

        when(registryService.getRequired(datasetId)).thenReturn(dataset);
        when(hdfsStore.exists("/datasets/values")).thenReturn(true);
        when(hdfsStore.listFilePaths("/datasets/values", 5000)).thenReturn(List.of("/datasets/values/data.csv"));
        when(hdfsStore.open("/datasets/values/data.csv")).thenReturn(new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)));

        CsvAnalyticsSnapshot snapshot = csvService.analyze(datasetId, null, true);

        assertThat(snapshot.getOverview().getDateColumn()).isNull();
        assertThat(snapshot.getOverview().getLocationColumn()).isEqualTo("Location");
        assertThat(snapshot.getMetricTotals())
                .extracting(item -> item.getName() + ":" + item.getCount())
                .containsExactly("Value:8");
        assertThat(snapshot.getTopLocationsByMetric())
                .singleElement()
                .extracting(breakdown -> breakdown.getItems())
                .asList()
                .extracting("name", "count")
                .containsExactly(tuple("US", 5L), tuple("Canada", 3L));
    }

    @Test
    void countsInvalidCsvFilesAsFailures() throws Exception {
        RegistryService registryService = mock(RegistryService.class);
        HdfsStore hdfsStore = mock(HdfsStore.class);
        CsvService csvService = new CsvService(registryService, hdfsStore, new AnalyticsProps());

        UUID datasetId = UUID.randomUUID();
        DatasetRegistration dataset = new DatasetRegistration(datasetId, "broken", "desc", DatasetType.CSV_TEXT, "/datasets/broken", Instant.now());

        when(registryService.getRequired(datasetId)).thenReturn(dataset);
        when(hdfsStore.exists("/datasets/broken")).thenReturn(true);
        when(hdfsStore.listFilePaths("/datasets/broken", 5000)).thenReturn(List.of("/datasets/broken/bad.csv", "/datasets/broken/empty.csv", "/datasets/broken/io.csv"));
        when(hdfsStore.open("/datasets/broken/bad.csv")).thenReturn(new ByteArrayInputStream("Name,Category\na,b\n".getBytes(StandardCharsets.UTF_8)));
        when(hdfsStore.open("/datasets/broken/empty.csv")).thenReturn(new ByteArrayInputStream(new byte[0]));
        when(hdfsStore.open("/datasets/broken/io.csv")).thenThrow(new IOException("boom"));

        CsvAnalyticsSnapshot snapshot = csvService.analyze(datasetId, null, true);

        assertThat(snapshot.getOverview().getScannedFiles()).isEqualTo(3);
        assertThat(snapshot.getOverview().getFailedFiles()).isEqualTo(3);
        assertThat(snapshot.getOverview().getProcessedRows()).isZero();
        assertThat(snapshot.getMetricTotals()).isEmpty();
    }

    @Test
    void analyzesXlsDataset() throws Exception {
        RegistryService registryService = mock(RegistryService.class);
        HdfsStore hdfsStore = mock(HdfsStore.class);
        AnalyticsProps analyticsProps = new AnalyticsProps();
        analyticsProps.setCacheTtl(Duration.ofMinutes(5));
        CsvService csvService = new CsvService(registryService, hdfsStore, analyticsProps);

        UUID datasetId = UUID.randomUUID();
        DatasetRegistration dataset = new DatasetRegistration(
                datasetId,
                "covid-xls",
                "Legacy spreadsheet report",
                DatasetType.CSV_TEXT,
                "/datasets/covid-xls",
                Instant.now()
        );

        when(registryService.getRequired(datasetId)).thenReturn(dataset);
        when(hdfsStore.exists("/datasets/covid-xls")).thenReturn(true);
        when(hdfsStore.listFilePaths("/datasets/covid-xls", analyticsProps.getDefaultMaxFiles()))
                .thenReturn(List.of("/datasets/covid-xls/daily.xls"));
        when(hdfsStore.open("/datasets/covid-xls/daily.xls"))
                .thenReturn(new ByteArrayInputStream(buildXlsWorkbook()));

        CsvAnalyticsSnapshot snapshot = csvService.analyze(datasetId, null, true);

        assertThat(snapshot.getDatasetType()).isEqualTo(DatasetType.CSV_TEXT);
        assertThat(snapshot.getOverview().getProcessedRows()).isEqualTo(4);
        assertThat(snapshot.getOverview().getDistinctLocations()).isEqualTo(2);
        assertThat(snapshot.getOverview().getDateColumn()).isEqualTo("ObservationDate");
        assertThat(snapshot.getOverview().getLocationColumn()).isEqualTo("Country/Region");
        assertThat(snapshot.getMetricTotals())
                .extracting(item -> item.getName() + ":" + item.getCount())
                .contains("Confirmed:3", "Deaths:0", "Recovered:0");
        assertThat(snapshot.getRowsByDate())
                .extracting(point -> point.getBucket() + ":" + point.getCount())
                .containsExactly("2020-01-22:2", "2020-01-23:2");
    }

    private byte[] buildXlsWorkbook() throws IOException {
        try (Workbook workbook = new HSSFWorkbook();
             ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            Sheet sheet = workbook.createSheet("daily");
            sheet.createRow(0).createCell(0).setCellValue("ObservationDate");
            sheet.getRow(0).createCell(1).setCellValue("Country/Region");
            sheet.getRow(0).createCell(2).setCellValue("Confirmed");
            sheet.getRow(0).createCell(3).setCellValue("Deaths");
            sheet.getRow(0).createCell(4).setCellValue("Recovered");

            Object[][] rows = new Object[][]{
                    {"1/22/2020", "US", 1, 0, 0},
                    {"1/22/2020", "Canada", 0, 0, 0},
                    {"1/23/2020", "US", 2, 0, 0},
                    {"1/23/2020", "Canada", 1, 0, 0}
            };

            for (int index = 0; index < rows.length; index++) {
                var row = sheet.createRow(index + 1);
                row.createCell(0).setCellValue((String) rows[index][0]);
                row.createCell(1).setCellValue((String) rows[index][1]);
                row.createCell(2).setCellValue(((Number) rows[index][2]).doubleValue());
                row.createCell(3).setCellValue(((Number) rows[index][3]).doubleValue());
                row.createCell(4).setCellValue(((Number) rows[index][4]).doubleValue());
            }

            workbook.write(outputStream);
            return outputStream.toByteArray();
        }
    }
}
