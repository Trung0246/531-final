package com.datasetviz.service;

import com.datasetviz.dto.DashboardProgressEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Service
public class DashboardProgressService {

    private final ObjectMapper objectMapper;
    private final ConcurrentMap<String, Set<WebSocketSession>> sessionsByDataset = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, DashboardProgressEvent> latestEventByDataset = new ConcurrentHashMap<>();

    public DashboardProgressService() {
        this(new ObjectMapper());
    }

    public DashboardProgressService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public void subscribe(String datasetId, WebSocketSession session) {
        sessionsByDataset.computeIfAbsent(datasetId, ignored -> ConcurrentHashMap.newKeySet()).add(session);
        DashboardProgressEvent latestEvent = latestEventByDataset.get(datasetId);
        publishToSession(session, latestEvent == null
                ? new DashboardProgressEvent(datasetId, "connected", "Live dashboard progress connected.", 0, 0, 0, 0, List.of(), List.of(), null, false)
                : lightweightReplay(latestEvent));
    }

    public void unsubscribe(WebSocketSession session) {
        sessionsByDataset.values().forEach(sessions -> sessions.remove(session));
    }

    public void publish(DashboardProgressEvent event) {
        latestEventByDataset.put(event.datasetId(), event);
        Set<WebSocketSession> sessions = sessionsByDataset.get(event.datasetId());
        if (sessions == null || sessions.isEmpty()) {
            return;
        }

        sessions.removeIf(session -> !session.isOpen());
        sessions.forEach(session -> publishToSession(session, event));
    }

    private void publishToSession(WebSocketSession session, DashboardProgressEvent event) {
        if (!session.isOpen()) {
            return;
        }

        try {
            synchronized (session) {
                session.sendMessage(new TextMessage(objectMapper.writeValueAsString(event)));
            }
        } catch (IOException exception) {
            unsubscribe(session);
        }
    }

    private DashboardProgressEvent lightweightReplay(DashboardProgressEvent event) {
        return new DashboardProgressEvent(
                event.datasetId(),
                event.complete() ? event.stage() : "replay",
                event.complete() ? event.message() : "Last known dashboard progress. Waiting for live updates...",
                event.scannedFiles(),
                event.totalFiles(),
                event.processedRows(),
                event.failedFiles(),
                event.files(),
                List.of(),
                null,
                event.complete()
        );
    }
}
