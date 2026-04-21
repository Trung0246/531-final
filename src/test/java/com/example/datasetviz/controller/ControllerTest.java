package com.example.datasetviz.controller;

import com.example.datasetviz.config.AnalyticsProperties;
import com.example.datasetviz.dto.ImportLocalDirectoryRequest;
import com.example.datasetviz.dto.RegisterDatasetRequest;
import com.example.datasetviz.model.AnalyticsOverview;
import com.example.datasetviz.model.CommunicationEdge;
import com.example.datasetviz.model.DatasetRegistration;
import com.example.datasetviz.model.DatasetType;
import com.example.datasetviz.model.EmailAnalyticsSnapshot;
import com.example.datasetviz.model.HdfsFileDescriptor;
import com.example.datasetviz.model.NamedCount;
import com.example.datasetviz.model.TimeSeriesPoint;
import com.example.datasetviz.service.DatasetAnalyticsService;
import com.example.datasetviz.service.DatasetImportService;
import com.example.datasetviz.service.DatasetRegistryService;
import com.example.datasetviz.service.EmailAnalyticsService;
import com.example.datasetviz.service.HdfsStorageService;
import org.junit.jupiter.api.Test;
import org.springframework.core.MethodParameter;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;

import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ControllerTest {

    @Test
    void analyticsControllerRoutesAndSlicesResponses() throws Exception {
        DatasetAnalyticsService datasetAnalyticsService = mock(DatasetAnalyticsService.class);
        EmailAnalyticsService emailAnalyticsService = mock(EmailAnalyticsService.class);
        AnalyticsProperties analyticsProperties = new AnalyticsProperties();
        analyticsProperties.setDefaultTopLimit(2);
        analyticsProperties.setDefaultGraphEdgeLimit(1);

        AnalyticsController controller = new AnalyticsController(datasetAnalyticsService, emailAnalyticsService, analyticsProperties);
        UUID datasetId = UUID.randomUUID();

        EmailAnalyticsSnapshot snapshot = new EmailAnalyticsSnapshot();
        snapshot.setOverview(new AnalyticsOverview());
        snapshot.setVolumeByMonth(List.of(new TimeSeriesPoint("2024-01", 1)));
        snapshot.setHourlyDistribution(List.of(new TimeSeriesPoint("01", 2)));
        snapshot.setTopSenders(List.of(new NamedCount("a", 5), new NamedCount("b", 4), new NamedCount("c", 3)));
        snapshot.setTopRecipients(List.of(new NamedCount("x", 5), new NamedCount("y", 4), new NamedCount("z", 3)));
        snapshot.setTopMailboxOwners(List.of(new NamedCount("owner1", 6), new NamedCount("owner2", 5), new NamedCount("owner3", 4)));
        snapshot.setTopSubjectKeywords(List.of(new NamedCount("k1", 7), new NamedCount("k2", 6), new NamedCount("k3", 5)));
        snapshot.setCommunicationGraph(List.of(new CommunicationEdge("s1", "t1", 1), new CommunicationEdge("s2", "t2", 2)));

        when(datasetAnalyticsService.analyze(datasetId, 9, true)).thenReturn(Map.of("ok", true));
        when(emailAnalyticsService.analyze(eq(datasetId), nullable(Integer.class), anyBoolean())).thenReturn(snapshot);

        assertThat(controller.analytics(datasetId, 9, true)).isEqualTo(Map.of("ok", true));
        assertThat(controller.overview(datasetId, null, false)).isSameAs(snapshot.getOverview());
        assertThat(controller.volumeByMonth(datasetId, null, false)).containsExactlyElementsOf(snapshot.getVolumeByMonth());
        assertThat(controller.hourlyDistribution(datasetId, null, false)).containsExactlyElementsOf(snapshot.getHourlyDistribution());
        assertThat(controller.topSenders(datasetId, null, null, false)).hasSize(2);
        assertThat(controller.topRecipients(datasetId, null, 1, false)).hasSize(1);
        assertThat(controller.topMailboxOwners(datasetId, null, null, false)).hasSize(2);
        assertThat(controller.subjectKeywords(datasetId, null, 3, false)).hasSize(3);
        assertThat(controller.communicationGraph(datasetId, null, null, false)).hasSize(1);
    }

    @Test
    void datasetControllerHandlesRegistrationImportAndQueries() throws Exception {
        DatasetRegistryService datasetRegistryService = mock(DatasetRegistryService.class);
        DatasetImportService datasetImportService = mock(DatasetImportService.class);
        HdfsStorageService hdfsStorageService = mock(HdfsStorageService.class);
        DatasetController controller = new DatasetController(datasetRegistryService, datasetImportService, hdfsStorageService);

        UUID datasetId = UUID.randomUUID();
        DatasetRegistration registration = new DatasetRegistration(datasetId, "dataset", "desc", DatasetType.EMAIL_ARCHIVE, "/path", Instant.now());
        RegisterDatasetRequest registerRequest = new RegisterDatasetRequest();
        registerRequest.setHdfsPath("/path");
        ImportLocalDirectoryRequest importRequest = new ImportLocalDirectoryRequest();
        HdfsFileDescriptor descriptor = new HdfsFileDescriptor("/path/file.txt", "file.txt", false, 1L, Instant.now());

        when(hdfsStorageService.exists("/path")).thenReturn(true);
        when(datasetRegistryService.register(registerRequest)).thenReturn(registration);
        when(datasetImportService.importLocalDirectory(importRequest)).thenReturn(registration);
        when(datasetRegistryService.listAll()).thenReturn(List.of(registration));
        when(datasetRegistryService.getRequired(datasetId)).thenReturn(registration);
        when(hdfsStorageService.listFiles("/path", false, 1)).thenReturn(List.of(descriptor));

        assertThat(controller.register(registerRequest).getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(controller.register(registerRequest).getBody()).isSameAs(registration);
        assertThat(controller.importLocal(importRequest).getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(controller.listDatasets()).containsExactly(registration);
        assertThat(controller.getDataset(datasetId)).isSameAs(registration);
        assertThat(controller.listFiles(datasetId, 0, false)).containsExactly(descriptor);
    }

    @Test
    void datasetControllerRejectsMissingHdfsPath() throws Exception {
        DatasetController controller = new DatasetController(mock(DatasetRegistryService.class), mock(DatasetImportService.class), mock(HdfsStorageService.class));
        RegisterDatasetRequest request = new RegisterDatasetRequest();
        request.setHdfsPath("/missing");
        when(controllerHdfs(controller).exists("/missing")).thenReturn(false);

        assertThatThrownBy(() -> controller.register(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("HDFS path does not exist");
    }

    @Test
    void globalExceptionHandlerBuildsProblemDetails() throws Exception {
        GlobalExceptionHandler handler = new GlobalExceptionHandler();

        ProblemDetail notFound = handler.handleNotFound(new NoSuchElementException("missing"));
        assertThat(notFound.getStatus()).isEqualTo(HttpStatus.NOT_FOUND.value());
        assertThat(notFound.getTitle()).isEqualTo("Resource not found");

        ProblemDetail badRequest = handler.handleBadRequest(new IllegalArgumentException("bad"));
        assertThat(badRequest.getStatus()).isEqualTo(HttpStatus.BAD_REQUEST.value());
        assertThat(badRequest.getTitle()).isEqualTo("Invalid request");

        BeanPropertyBindingResult bindingResult = new BeanPropertyBindingResult(new Object(), "target");
        bindingResult.addError(new FieldError("target", "name", "invalid"));
        bindingResult.addError(new FieldError("target", "name", "invalid-again"));
        bindingResult.addError(new FieldError("target", "path", "invalid"));
        ProblemDetail validation = handler.handleValidation(new MethodArgumentNotValidException(sampleMethodParameter(), bindingResult));
        assertThat(validation.getDetail()).isEqualTo("Invalid fields: name, path");

        BeanPropertyBindingResult blankBindingResult = new BeanPropertyBindingResult(new Object(), "target");
        ProblemDetail blankValidation = handler.handleValidation(new MethodArgumentNotValidException(sampleMethodParameter(), blankBindingResult));
        assertThat(blankValidation.getDetail()).isEqualTo("Request validation failed");

        ProblemDetail io = handler.handleIo(new IOException("disk"));
        assertThat(io.getStatus()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR.value());
        assertThat(io.getTitle()).isEqualTo("I/O error");
    }

    private static HdfsStorageService controllerHdfs(DatasetController controller) throws Exception {
        java.lang.reflect.Field field = DatasetController.class.getDeclaredField("hdfsStorageService");
        field.setAccessible(true);
        return (HdfsStorageService) field.get(controller);
    }

    private static MethodParameter sampleMethodParameter() throws NoSuchMethodException {
        Method method = ControllerTest.class.getDeclaredMethod("sampleMethod", String.class);
        return new MethodParameter(method, 0);
    }

    @SuppressWarnings("unused")
    private static void sampleMethod(String value) {
    }
}
