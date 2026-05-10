package com.datasetviz.service;

import com.datasetviz.model.DatasetRegistration;
import com.datasetviz.model.DatasetType;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.UUID;

@Service
public class AnalyticsService {

    private final RegistryService registry;
    private final EmailService email;
    private final CsvService csv;
    private final StateService state;

    public AnalyticsService(RegistryService registry,
                                   EmailService email,
                                   CsvService csv,
                                   StateService state) {
        this.registry = registry;
        this.email = email;
        this.csv = csv;
        this.state = state;
    }

    public Object analyze(UUID datasetId, Integer maxReq, boolean refresh) throws IOException {
        return analyze(datasetId, maxReq, null, refresh);
    }

    public Object analyze(UUID datasetId, Integer maxReq, Integer rowReq, boolean refresh) throws IOException {
        return analyze(datasetId, maxReq, rowReq, null, refresh);
    }

    public Object analyze(UUID datasetId,
                          Integer maxReq,
                          Integer rowReq,
                          Integer dashReq,
                          boolean refresh) throws IOException {
        DatasetRegistration dataset = registry.getRequired(datasetId);
        return switch (dataset.getDatasetType()) {
            case EMAIL_ARCHIVE -> email.analyze(datasetId, maxReq, refresh);
            case CSV_TEXT -> csv.analyze(datasetId, maxReq, rowReq, dashReq, refresh);
            default -> throw new IllegalArgumentException("Current analytics implementation supports EMAIL_ARCHIVE and CSV_TEXT datasets only.");
        };
    }

    public boolean cancel(UUID datasetId) {
        registry.getRequired(datasetId);
        return state.cancel(datasetId);
    }

    public void invalidateCache(UUID datasetId) {
        email.invalidateCache(datasetId);
        csv.invalidateCache(datasetId);
    }
}
