package com.datasetviz.controller;

import com.datasetviz.config.AnalyticsProperties;
import com.datasetviz.dto.DashboardView;
import com.datasetviz.dto.DatasetView;
import com.datasetviz.dto.ImportLocalDirectoryRequest;
import com.datasetviz.dto.RegisterDatasetRequest;
import com.datasetviz.model.AnalyticsOverview;
import com.datasetviz.model.CommunicationEdge;
import com.datasetviz.model.DatasetRegistration;
import com.datasetviz.model.DatasetType;
import com.datasetviz.model.EmailAnalyticsSnapshot;
import com.datasetviz.model.HdfsFileDescriptor;
import com.datasetviz.model.NamedCount;
import com.datasetviz.model.TimeSeriesPoint;
import com.datasetviz.service.DashboardViewService;
import com.datasetviz.service.DatasetAnalyticsService;
import com.datasetviz.service.DatasetImportService;
import com.datasetviz.service.DatasetRegistryService;
import com.datasetviz.service.EmailAnalyticsService;
import com.datasetviz.service.HdfsStorageService;
import org.junit.jupiter.api.Test;
import org.springframework.core.MethodParameter;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.multipart.MaxUploadSizeExceededException;

import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
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
        when(datasetImportService.importLocalDirectory(importRequest)).thenReturn(List.of(descriptor));
        when(datasetRegistryService.listAll()).thenReturn(List.of(registration));
        when(datasetRegistryService.getRequired(datasetId)).thenReturn(registration);
        when(hdfsStorageService.listFiles("/path", false, 1)).thenReturn(List.of(descriptor));

        assertThat(controller.register(registerRequest).getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(controller.register(registerRequest).getBody()).isSameAs(registration);
        assertThat(controller.importLocal(importRequest).getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(controller.importLocal(importRequest).getBody()).containsExactly(descriptor);
        assertThat(controller.listDatasets()).containsExactly(registration);
        assertThat(controller.getDataset(datasetId)).isSameAs(registration);
        assertThat(controller.listFiles(datasetId, 0, false)).containsExactly(descriptor);
    }

    @Test
    void datasetControllerCreatesMissingHdfsPath() throws Exception {
        DatasetController controller = new DatasetController(mock(DatasetRegistryService.class), mock(DatasetImportService.class), mock(HdfsStorageService.class));
        RegisterDatasetRequest request = new RegisterDatasetRequest();
        request.setHdfsPath("/missing");
        when(controllerHdfs(controller).exists("/missing")).thenReturn(false);

        controller.register(request);

        verify(controllerHdfs(controller)).createDirectories("/missing");
    }

    @Test
    void datasetGraphqlControllerQueriesAndMutates() throws Exception {
        DatasetRegistryService datasetRegistryService = mock(DatasetRegistryService.class);
        DatasetAnalyticsService datasetAnalyticsService = mock(DatasetAnalyticsService.class);
        DashboardViewService dashboardViewService = mock(DashboardViewService.class);
        HdfsStorageService hdfsStorageService = mock(HdfsStorageService.class);
        DatasetGraphqlController controller = new DatasetGraphqlController(datasetRegistryService, datasetAnalyticsService, dashboardViewService, hdfsStorageService);

        UUID datasetId = UUID.randomUUID();
        DatasetRegistration registration = new DatasetRegistration(datasetId, "dataset", "desc", DatasetType.EMAIL_ARCHIVE, "/path", Instant.now());
        DatasetView datasetView = new DatasetView(datasetId.toString(), "dataset", "desc", DatasetType.EMAIL_ARCHIVE, "/path", "2024-01-01T00:00:00Z", true);
        DashboardView dashboardView = new DashboardView(
                datasetId.toString(),
                "dataset",
                DatasetType.EMAIL_ARCHIVE,
                "/path",
                "2024-01-01T00:00:00Z",
                10,
                List.of(new DashboardView.SummaryItem("Dataset", "dataset")),
                List.of(),
                List.of(),
                null,
                null
        );
        Object snapshot = new Object();
        RegisterDatasetRequest request = new RegisterDatasetRequest();
        request.setName("dataset");
        request.setHdfsPath("/path");

        when(datasetRegistryService.listAll()).thenReturn(List.of(registration));
        when(dashboardViewService.toDatasetView(registration)).thenReturn(datasetView);
        when(datasetAnalyticsService.analyze(datasetId, null, false)).thenReturn(snapshot);
        when(dashboardViewService.toDashboardView(snapshot)).thenReturn(dashboardView);
        when(hdfsStorageService.exists("/path")).thenReturn(true);
        when(datasetRegistryService.register(request)).thenReturn(registration);
        when(dashboardViewService.toDatasetView(registration, true)).thenReturn(datasetView);

        assertThat(controller.datasets()).containsExactly(datasetView);
        assertThat(controller.dashboard(datasetId.toString(), null, null)).isSameAs(dashboardView);
        assertThat(controller.registerDataset(request)).isSameAs(datasetView);
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

        ProblemDetail tooLarge = handler.handleMaxUploadSize(new MaxUploadSizeExceededException(1));
        assertThat(tooLarge.getStatus()).isEqualTo(HttpStatus.PAYLOAD_TOO_LARGE.value());
        assertThat(tooLarge.getTitle()).isEqualTo("Upload too large");

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
