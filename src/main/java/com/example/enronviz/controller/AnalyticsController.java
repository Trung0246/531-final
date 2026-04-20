package com.example.enronviz.controller;

import com.example.enronviz.config.AnalyticsProperties;
import com.example.enronviz.model.AnalyticsOverview;
import com.example.enronviz.model.CommunicationEdge;
import com.example.enronviz.model.EmailAnalyticsSnapshot;
import com.example.enronviz.model.NamedCount;
import com.example.enronviz.model.TimeSeriesPoint;
import com.example.enronviz.service.EmailAnalyticsService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/datasets/{datasetId}/analytics")
public class AnalyticsController {

    private final EmailAnalyticsService emailAnalyticsService;
    private final AnalyticsProperties analyticsProperties;

    public AnalyticsController(EmailAnalyticsService emailAnalyticsService,
                               AnalyticsProperties analyticsProperties) {
        this.emailAnalyticsService = emailAnalyticsService;
        this.analyticsProperties = analyticsProperties;
    }

    @GetMapping
    public EmailAnalyticsSnapshot analytics(@PathVariable UUID datasetId,
                                            @RequestParam(required = false) Integer maxFiles,
                                            @RequestParam(defaultValue = "false") boolean refresh) throws IOException {
        return emailAnalyticsService.analyze(datasetId, maxFiles, refresh);
    }

    @GetMapping("/overview")
    public AnalyticsOverview overview(@PathVariable UUID datasetId,
                                      @RequestParam(required = false) Integer maxFiles,
                                      @RequestParam(defaultValue = "false") boolean refresh) throws IOException {
        return emailAnalyticsService.analyze(datasetId, maxFiles, refresh).getOverview();
    }

    @GetMapping("/volume-by-month")
    public List<TimeSeriesPoint> volumeByMonth(@PathVariable UUID datasetId,
                                               @RequestParam(required = false) Integer maxFiles,
                                               @RequestParam(defaultValue = "false") boolean refresh) throws IOException {
        return emailAnalyticsService.analyze(datasetId, maxFiles, refresh).getVolumeByMonth();
    }

    @GetMapping("/hourly-distribution")
    public List<TimeSeriesPoint> hourlyDistribution(@PathVariable UUID datasetId,
                                                    @RequestParam(required = false) Integer maxFiles,
                                                    @RequestParam(defaultValue = "false") boolean refresh) throws IOException {
        return emailAnalyticsService.analyze(datasetId, maxFiles, refresh).getHourlyDistribution();
    }

    @GetMapping("/top-senders")
    public List<NamedCount> topSenders(@PathVariable UUID datasetId,
                                       @RequestParam(required = false) Integer maxFiles,
                                       @RequestParam(required = false) Integer limit,
                                       @RequestParam(defaultValue = "false") boolean refresh) throws IOException {
        return slice(emailAnalyticsService.analyze(datasetId, maxFiles, refresh).getTopSenders(),
                limit,
                analyticsProperties.getDefaultTopLimit());
    }

    @GetMapping("/top-recipients")
    public List<NamedCount> topRecipients(@PathVariable UUID datasetId,
                                          @RequestParam(required = false) Integer maxFiles,
                                          @RequestParam(required = false) Integer limit,
                                          @RequestParam(defaultValue = "false") boolean refresh) throws IOException {
        return slice(emailAnalyticsService.analyze(datasetId, maxFiles, refresh).getTopRecipients(),
                limit,
                analyticsProperties.getDefaultTopLimit());
    }

    @GetMapping("/top-mailbox-owners")
    public List<NamedCount> topMailboxOwners(@PathVariable UUID datasetId,
                                             @RequestParam(required = false) Integer maxFiles,
                                             @RequestParam(required = false) Integer limit,
                                             @RequestParam(defaultValue = "false") boolean refresh) throws IOException {
        return slice(emailAnalyticsService.analyze(datasetId, maxFiles, refresh).getTopMailboxOwners(),
                limit,
                analyticsProperties.getDefaultTopLimit());
    }

    @GetMapping("/subject-keywords")
    public List<NamedCount> subjectKeywords(@PathVariable UUID datasetId,
                                            @RequestParam(required = false) Integer maxFiles,
                                            @RequestParam(required = false) Integer limit,
                                            @RequestParam(defaultValue = "false") boolean refresh) throws IOException {
        return slice(emailAnalyticsService.analyze(datasetId, maxFiles, refresh).getTopSubjectKeywords(),
                limit,
                analyticsProperties.getDefaultTopLimit());
    }

    @GetMapping("/communication-graph")
    public List<CommunicationEdge> communicationGraph(@PathVariable UUID datasetId,
                                                      @RequestParam(required = false) Integer maxFiles,
                                                      @RequestParam(required = false) Integer limit,
                                                      @RequestParam(defaultValue = "false") boolean refresh) throws IOException {
        return slice(emailAnalyticsService.analyze(datasetId, maxFiles, refresh).getCommunicationGraph(),
                limit,
                analyticsProperties.getDefaultGraphEdgeLimit());
    }

    private <T> List<T> slice(List<T> values, Integer requestedLimit, int defaultLimit) {
        int safeLimit = requestedLimit == null || requestedLimit < 1 ? defaultLimit : requestedLimit;
        return values.stream().limit(safeLimit).toList();
    }
}
