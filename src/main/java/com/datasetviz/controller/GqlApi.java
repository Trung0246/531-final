package com.datasetviz.controller;

import com.datasetviz.dto.DashboardView;
import com.datasetviz.dto.DatasetView;
import com.datasetviz.dto.RegisterDatasetRequest;
import com.datasetviz.service.ViewService;
import com.datasetviz.service.AnalyticsService;
import com.datasetviz.service.RegistryService;
import com.datasetviz.service.HdfsStore;
import jakarta.validation.Valid;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

@Controller
public class GqlApi {

    private final RegistryService registry;
    private final AnalyticsService analytics;
    private final ViewService views;
    private final HdfsStore hdfs;

    public GqlApi(RegistryService registry,
                                    AnalyticsService analytics,
                                    ViewService views,
                                    HdfsStore hdfs) {
        this.registry = registry;
        this.analytics = analytics;
        this.views = views;
        this.hdfs = hdfs;
    }

    @QueryMapping
    public List<DatasetView> datasets() {
        return registry.listAll().stream()
                .map(views::toDatasetView)
                .toList();
    }

    @QueryMapping
    public DashboardView dashboard(@Argument String datasetId,
                                    @Argument Integer maxFiles,
                                    @Argument Integer updateEveryRows,
                                    @Argument Integer fullDashboardUpdateEveryRows,
                                    @Argument Boolean refresh) throws IOException {
        return views.toDashboardView(
                analytics.analyze(UUID.fromString(datasetId), maxFiles, updateEveryRows, fullDashboardUpdateEveryRows, Boolean.TRUE.equals(refresh))
        );
    }

    @MutationMapping
    public DatasetView registerDataset(@Argument("input") @Valid RegisterDatasetRequest input) throws IOException {
        boolean existed = hdfs.exists(input.getHdfsPath());
        if (!existed) {
            hdfs.createDirectories(input.getHdfsPath());
        }
        return views.toDatasetView(registry.register(input), existed);
    }

}
