package com.datasetviz.service;

import com.datasetviz.config.AnalyticsProperties;
import com.datasetviz.model.EmailRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ServiceInternalsCoverageTest {

    @Test
    void csvServicePrivateMethodsCoverRemainingBranches() throws Exception {
        AnalyticsProperties analyticsProperties = new AnalyticsProperties();
        analyticsProperties.setDefaultMaxFiles(5);
        analyticsProperties.setMaxFilesHardLimit(10);
        CsvAnalyticsService service = new CsvAnalyticsService(mock(DatasetRegistryService.class), mock(HdfsStorageService.class), analyticsProperties);

        assertThat(invoke(service, "resolveMaxFiles", new Class[]{Integer.class}, 0)).isEqualTo(5);
        assertThat(invoke(service, "resolveMaxFiles", new Class[]{Integer.class}, 99)).isEqualTo(10);
        assertThat(invoke(service, "normalizeHeader", new Class[]{String.class}, new Object[]{null})).isEqualTo("");

        assertThat(invokeStatic(CsvAnalyticsService.class, "parseDate", new Class[]{String.class}, "2020-01-01T10:15:30Z[UTC]")).isEqualTo(Optional.of(LocalDate.parse("2020-01-01")));
        assertThat(invokeStatic(CsvAnalyticsService.class, "parseDate", new Class[]{String.class}, "2020-01-01T10:15:30+02:00")).isEqualTo(Optional.of(LocalDate.parse("2020-01-01")));
        assertThat(invokeStatic(CsvAnalyticsService.class, "parseDate", new Class[]{String.class}, "2020-01-01T10:15:30")).isEqualTo(Optional.of(LocalDate.parse("2020-01-01")));
        assertThat(invokeStatic(CsvAnalyticsService.class, "parseDate", new Class[]{String.class}, "2020-01-01 extra")).isEqualTo(Optional.of(LocalDate.parse("2020-01-01")));
        assertThat(invokeStatic(CsvAnalyticsService.class, "parseDate", new Class[]{String.class}, "   ")).isEqualTo(Optional.empty());
        assertThat(invokeStatic(CsvAnalyticsService.class, "parseDate", new Class[]{String.class}, "nope")).isEqualTo(Optional.empty());
        assertThat(invokeStatic(CsvAnalyticsService.class, "parseNumericValue", new Class[]{String.class}, "   ")).isEqualTo(Optional.empty());

        CSVRecord unmappedRecord = csvRecords("Other\n1\n").get(0);
        assertThat(invoke(service, "isNumericColumn", new Class[]{String.class, List.class}, "Value", List.of(unmappedRecord))).isEqualTo(false);

        CSVRecord blankRecord = csvRecords("Value\n \n").get(0);
        assertThat(invoke(service, "isNumericColumn", new Class[]{String.class, List.class}, "Value", List.of(blankRecord))).isEqualTo(false);

        CSVRecord numericRecord = csvRecords("Value\n7\n").get(0);
        assertThat(invoke(service, "isNumericColumn", new Class[]{String.class, List.class}, "Value", List.of(numericRecord))).isEqualTo(true);
        assertThat(invoke(service, "isNumericColumn", new Class[]{String.class, List.class}, "Value", java.util.Collections.nCopies(25, numericRecord))).isEqualTo(true);

        assertThat(invokeStatic(CsvAnalyticsService.class, "resolveLocation", new Class[]{CSVRecord.class, String.class}, blankRecord, null)).isNull();
        CSVRecord blankLocationRecord = csvRecords("Location\n \n").get(0);
        assertThat(invokeStatic(CsvAnalyticsService.class, "resolveLocation", new Class[]{CSVRecord.class, String.class}, blankLocationRecord, "Location")).isNull();

        Class<?> fileSchemaClass = Class.forName("com.datasetviz.service.CsvAnalyticsService$FileSchema");
        Constructor<?> fileSchemaConstructor = fileSchemaClass.getDeclaredConstructor(String.class, String.class, List.class);
        fileSchemaConstructor.setAccessible(true);
        Object schema = fileSchemaConstructor.newInstance("Date", null, List.of("Value"));

        Class<?> mutableAnalyticsClass = Class.forName("com.datasetviz.service.CsvAnalyticsService$MutableAnalytics");
        Constructor<?> mutableAnalyticsConstructor = mutableAnalyticsClass.getDeclaredConstructor();
        mutableAnalyticsConstructor.setAccessible(true);
        Object mutableAnalytics = mutableAnalyticsConstructor.newInstance();

        Method acceptFile = mutableAnalyticsClass.getDeclaredMethod("acceptFile", fileSchemaClass, List.class);
        acceptFile.setAccessible(true);

        CSVRecord metricMissing = mock(CSVRecord.class);
        when(metricMissing.isMapped("Date")).thenReturn(false);
        when(metricMissing.isMapped("Value")).thenReturn(false);

        CSVRecord metricBlank = mock(CSVRecord.class);
        when(metricBlank.isMapped("Date")).thenReturn(false);
        when(metricBlank.isMapped("Value")).thenReturn(true);
        when(metricBlank.get("Value")).thenReturn(" ");

        acceptFile.invoke(mutableAnalytics, schema, List.of(metricMissing, metricBlank));
    }

    @Test
    void emailAnalyticsMutableAnalyticsCoversNormalizationAndDateComparisons() throws Exception {
        Class<?> mutableAnalyticsClass = Class.forName("com.datasetviz.service.EmailAnalyticsService$MutableAnalytics");
        Constructor<?> constructor = mutableAnalyticsClass.getDeclaredConstructor();
        constructor.setAccessible(true);
        Object mutableAnalytics = constructor.newInstance();

        Method normalize = mutableAnalyticsClass.getDeclaredMethod("normalize", String.class);
        normalize.setAccessible(true);
        assertThat(normalize.invoke(mutableAnalytics, new Object[]{null})).isNull();

        Method normalizeList = mutableAnalyticsClass.getDeclaredMethod("normalizeList", List.class);
        normalizeList.setAccessible(true);
        assertThat(normalizeList.invoke(mutableAnalytics, List.of(" ", "User@Example.com"))).isEqualTo(List.of("user@example.com"));

        Method accept = mutableAnalyticsClass.getDeclaredMethod("accept", EmailRecord.class);
        accept.setAccessible(true);

        EmailRecord first = new EmailRecord();
        first.setFrom("user@example.com");
        first.setTo(List.of("alpha@example.com"));
        first.setSubject("First message");
        first.setMailboxOwner("owner");
        first.setSentAt(Instant.parse("2024-01-02T10:00:00Z"));

        EmailRecord earlier = new EmailRecord();
        earlier.setFrom("user@example.com");
        earlier.setTo(List.of("beta@example.com"));
        earlier.setSubject("Earlier message");
        earlier.setMailboxOwner("owner");
        earlier.setSentAt(Instant.parse("2024-01-01T10:00:00Z"));

        EmailRecord later = new EmailRecord();
        later.setFrom(null);
        later.setTo(List.of(" ", "gamma@example.com"));
        later.setSubject("Later message");
        later.setMailboxOwner("owner");
        later.setSentAt(Instant.parse("2024-01-03T10:00:00Z"));

        accept.invoke(mutableAnalytics, first);
        accept.invoke(mutableAnalytics, earlier);
        accept.invoke(mutableAnalytics, later);
    }

    private static Object invoke(Object target, String methodName, Class<?>[] parameterTypes, Object... args) throws Exception {
        Method method = target.getClass().getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(target, args);
    }

    private static Object invokeStatic(Class<?> type, String methodName, Class<?>[] parameterTypes, Object... args) throws Exception {
        Method method = type.getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(null, args);
    }

    private static List<CSVRecord> csvRecords(String csv) throws Exception {
        try (CSVParser parser = CSVFormat.DEFAULT.builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                .setIgnoreEmptyLines(true)
                .setTrim(true)
                .get()
                .parse(new StringReader(csv))) {
            return parser.getRecords();
        }
    }
}
