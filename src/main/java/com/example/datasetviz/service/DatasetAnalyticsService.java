package com.example.datasetviz.service;

import com.example.datasetviz.model.DatasetRegistration;
import com.example.datasetviz.model.DatasetType;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.UUID;

@Service
public class DatasetAnalyticsService {

    private final DatasetRegistryService datasetRegistryService;
    private final EmailAnalyticsService emailAnalyticsService;
    private final CsvAnalyticsService csvAnalyticsService;

    public DatasetAnalyticsService(DatasetRegistryService datasetRegistryService,
                                   EmailAnalyticsService emailAnalyticsService,
                                   CsvAnalyticsService csvAnalyticsService) {
        this.datasetRegistryService = datasetRegistryService;
        this.emailAnalyticsService = emailAnalyticsService;
        this.csvAnalyticsService = csvAnalyticsService;
    }

    public Object analyze(UUID datasetId, Integer requestedMaxFiles, boolean refresh) throws IOException {
        DatasetRegistration dataset = datasetRegistryService.getRequired(datasetId);
        return switch (dataset.getDatasetType()) {
            case EMAIL_ARCHIVE -> emailAnalyticsService.analyze(datasetId, requestedMaxFiles, refresh);
            case CSV_TEXT -> csvAnalyticsService.analyze(datasetId, requestedMaxFiles, refresh);
            default -> throw new IllegalArgumentException("Current analytics implementation supports EMAIL_ARCHIVE and CSV_TEXT datasets only.");
        };
    }
}
