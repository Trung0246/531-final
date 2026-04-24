package com.datasetviz.controller;

import com.datasetviz.dto.DashboardView;
import com.datasetviz.dto.DatasetView;
import com.datasetviz.dto.RegisterDatasetRequest;
import com.datasetviz.service.DashboardViewService;
import com.datasetviz.service.DatasetAnalyticsService;
import com.datasetviz.service.DatasetRegistryService;
import com.datasetviz.service.HdfsStorageService;
import jakarta.validation.Valid;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

@Controller
public class DatasetGraphqlController {

    private final DatasetRegistryService datasetRegistryService;
    private final DatasetAnalyticsService datasetAnalyticsService;
    private final DashboardViewService dashboardViewService;
    private final HdfsStorageService hdfsStorageService;

    public DatasetGraphqlController(DatasetRegistryService datasetRegistryService,
                                    DatasetAnalyticsService datasetAnalyticsService,
                                    DashboardViewService dashboardViewService,
                                    HdfsStorageService hdfsStorageService) {
        this.datasetRegistryService = datasetRegistryService;
        this.datasetAnalyticsService = datasetAnalyticsService;
        this.dashboardViewService = dashboardViewService;
        this.hdfsStorageService = hdfsStorageService;
    }

    @QueryMapping
    public List<DatasetView> datasets() {
        return datasetRegistryService.listAll().stream()
                .map(dashboardViewService::toDatasetView)
                .toList();
    }

    @QueryMapping
    public DashboardView dashboard(@Argument String datasetId,
                                   @Argument Integer maxFiles,
                                   @Argument Boolean refresh) throws IOException {
        return dashboardViewService.toDashboardView(
                datasetAnalyticsService.analyze(UUID.fromString(datasetId), maxFiles, Boolean.TRUE.equals(refresh))
        );
    }

    @MutationMapping
    public DatasetView registerDataset(@Argument("input") @Valid RegisterDatasetRequest input) throws IOException {
        boolean hdfsPathAlreadyExisted = hdfsStorageService.exists(input.getHdfsPath());
        if (!hdfsPathAlreadyExisted) {
            hdfsStorageService.createDirectories(input.getHdfsPath());
        }
        return dashboardViewService.toDatasetView(datasetRegistryService.register(input), hdfsPathAlreadyExisted);
    }

}
