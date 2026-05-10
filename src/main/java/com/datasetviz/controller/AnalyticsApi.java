package com.datasetviz.controller;

import com.datasetviz.config.AnalyticsProps;
import com.datasetviz.model.AnalyticsOverview;
import com.datasetviz.model.CommunicationEdge;
import com.datasetviz.model.NamedCount;
import com.datasetviz.model.TimeSeriesPoint;
import com.datasetviz.service.AnalyticsService;
import com.datasetviz.service.EmailService;
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
public class AnalyticsApi {

    private final AnalyticsService analytics;
    private final EmailService email;
    private final AnalyticsProps props;

    public AnalyticsApi(AnalyticsService analytics,
                               EmailService email,
                                AnalyticsProps props) {
        this.analytics = analytics;
        this.email = email;
        this.props = props;
    }

    @GetMapping
    public Object analytics(@PathVariable UUID datasetId,
                            @RequestParam(required = false) Integer maxFiles,
                            @RequestParam(defaultValue = "false") boolean refresh) throws IOException {
        return analytics.analyze(datasetId, maxFiles, refresh);
    }

    @GetMapping("/overview")
    public AnalyticsOverview overview(@PathVariable UUID datasetId,
                                      @RequestParam(required = false) Integer maxFiles,
                                      @RequestParam(defaultValue = "false") boolean refresh) throws IOException {
        return email.analyze(datasetId, maxFiles, refresh).getOverview();
    }

    @GetMapping("/volume-by-month")
    public List<TimeSeriesPoint> volumeByMonth(@PathVariable UUID datasetId,
                                               @RequestParam(required = false) Integer maxFiles,
                                               @RequestParam(defaultValue = "false") boolean refresh) throws IOException {
        return email.analyze(datasetId, maxFiles, refresh).getVolumeByMonth();
    }

    @GetMapping("/hourly-distribution")
    public List<TimeSeriesPoint> hourlyDistribution(@PathVariable UUID datasetId,
                                                    @RequestParam(required = false) Integer maxFiles,
                                                    @RequestParam(defaultValue = "false") boolean refresh) throws IOException {
        return email.analyze(datasetId, maxFiles, refresh).getHourlyDistribution();
    }

    @GetMapping("/top-senders")
    public List<NamedCount> topSenders(@PathVariable UUID datasetId,
                                       @RequestParam(required = false) Integer maxFiles,
                                       @RequestParam(required = false) Integer limit,
                                       @RequestParam(defaultValue = "false") boolean refresh) throws IOException {
        return slice(email.analyze(datasetId, maxFiles, refresh).getTopSenders(),
                limit,
                props.getDefaultTopLimit());
    }

    @GetMapping("/top-recipients")
    public List<NamedCount> topRecipients(@PathVariable UUID datasetId,
                                          @RequestParam(required = false) Integer maxFiles,
                                          @RequestParam(required = false) Integer limit,
                                          @RequestParam(defaultValue = "false") boolean refresh) throws IOException {
        return slice(email.analyze(datasetId, maxFiles, refresh).getTopRecipients(),
                limit,
                props.getDefaultTopLimit());
    }

    @GetMapping("/top-mailbox-owners")
    public List<NamedCount> topMailboxOwners(@PathVariable UUID datasetId,
                                             @RequestParam(required = false) Integer maxFiles,
                                             @RequestParam(required = false) Integer limit,
                                             @RequestParam(defaultValue = "false") boolean refresh) throws IOException {
        return slice(email.analyze(datasetId, maxFiles, refresh).getTopMailboxOwners(),
                limit,
                props.getDefaultTopLimit());
    }

    @GetMapping("/subject-keywords")
    public List<NamedCount> subjectKeywords(@PathVariable UUID datasetId,
                                            @RequestParam(required = false) Integer maxFiles,
                                            @RequestParam(required = false) Integer limit,
                                            @RequestParam(defaultValue = "false") boolean refresh) throws IOException {
        return slice(email.analyze(datasetId, maxFiles, refresh).getTopSubjectKeywords(),
                limit,
                props.getDefaultTopLimit());
    }

    @GetMapping("/communication-graph")
    public List<CommunicationEdge> communicationGraph(@PathVariable UUID datasetId,
                                                      @RequestParam(required = false) Integer maxFiles,
                                                      @RequestParam(required = false) Integer limit,
                                                      @RequestParam(defaultValue = "false") boolean refresh) throws IOException {
        return slice(email.analyze(datasetId, maxFiles, refresh).getCommunicationGraph(),
                limit,
                props.getDefaultGraphEdgeLimit());
    }

    private <T> List<T> slice(List<T> values, Integer requestedLimit, int defaultLimit) {
        int safeLimit = requestedLimit == null || requestedLimit < 1 ? defaultLimit : requestedLimit;
        return values.stream().limit(safeLimit).toList();
    }
}
