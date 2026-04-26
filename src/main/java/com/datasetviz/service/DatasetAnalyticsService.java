package com.datasetviz.service;

import com.datasetviz.model.DatasetRegistration;
import com.datasetviz.model.DatasetType;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.UUID;

@Service
public class DatasetAnalyticsService {

    private final DatasetRegistryService datasetRegistryService;
    private final EmailAnalyticsService emailAnalyticsService;
    private final CsvAnalyticsService csvAnalyticsService;
    private final DatasetProcessingStateService datasetProcessingStateService;

    public DatasetAnalyticsService(DatasetRegistryService datasetRegistryService,
                                   EmailAnalyticsService emailAnalyticsService,
                                   CsvAnalyticsService csvAnalyticsService,
                                   DatasetProcessingStateService datasetProcessingStateService) {
        this.datasetRegistryService = datasetRegistryService;
        this.emailAnalyticsService = emailAnalyticsService;
        this.csvAnalyticsService = csvAnalyticsService;
        this.datasetProcessingStateService = datasetProcessingStateService;
    }

    public Object analyze(UUID datasetId, Integer requestedMaxFiles, boolean refresh) throws IOException {
        return analyze(datasetId, requestedMaxFiles, null, refresh);
    }

    public Object analyze(UUID datasetId, Integer requestedMaxFiles, Integer requestedUpdateEveryRows, boolean refresh) throws IOException {
        return analyze(datasetId, requestedMaxFiles, requestedUpdateEveryRows, null, refresh);
    }

    public Object analyze(UUID datasetId,
                          Integer requestedMaxFiles,
                          Integer requestedUpdateEveryRows,
                          Integer requestedFullDashboardUpdateEveryRows,
                          boolean refresh) throws IOException {
        DatasetRegistration dataset = datasetRegistryService.getRequired(datasetId);
        return switch (dataset.getDatasetType()) {
            case EMAIL_ARCHIVE -> emailAnalyticsService.analyze(datasetId, requestedMaxFiles, refresh);
            case CSV_TEXT -> csvAnalyticsService.analyze(datasetId, requestedMaxFiles, requestedUpdateEveryRows, requestedFullDashboardUpdateEveryRows, refresh);
            default -> throw new IllegalArgumentException("Current analytics implementation supports EMAIL_ARCHIVE and CSV_TEXT datasets only.");
        };
    }

    public boolean cancel(UUID datasetId) {
        datasetRegistryService.getRequired(datasetId);
        return datasetProcessingStateService.cancelJob(datasetId);
    }
}
