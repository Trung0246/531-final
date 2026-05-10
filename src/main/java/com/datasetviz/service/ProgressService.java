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
public class ProgressService {

    private final ObjectMapper json;
    private final ConcurrentMap<String, Set<WebSocketSession>> sessions = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, DashboardProgressEvent> latest = new ConcurrentHashMap<>();

    public ProgressService() {
        this(new ObjectMapper());
    }

    public ProgressService(ObjectMapper json) {
        this.json = json;
    }

    public void subscribe(String datasetId, WebSocketSession session) {
        sessions.computeIfAbsent(datasetId, ignored -> ConcurrentHashMap.newKeySet()).add(session);
        DashboardProgressEvent last = latest.get(datasetId);
        send(session, last == null
                ? new DashboardProgressEvent(datasetId, "connected", "Live dashboard progress connected.", 0, 0, 0, 0, List.of(), List.of(), null, false)
                : replay(last));
    }

    public void unsubscribe(WebSocketSession session) {
        sessions.values().forEach(group -> group.remove(session));
    }

    public void emit(DashboardProgressEvent e) {
        latest.put(e.datasetId(), e);
        Set<WebSocketSession> peers = sessions.get(e.datasetId());
        if (peers == null || peers.isEmpty()) {
            return;
        }

        peers.removeIf(session -> !session.isOpen());
        peers.forEach(session -> send(session, e));
    }

    public void clear(String datasetId) {
        latest.remove(datasetId);
    }

    private void send(WebSocketSession session, DashboardProgressEvent e) {
        if (!session.isOpen()) {
            return;
        }

        try {
            synchronized (session) {
                session.sendMessage(new TextMessage(json.writeValueAsString(e)));
            }
        } catch (IOException exception) {
            unsubscribe(session);
        }
    }

    private DashboardProgressEvent replay(DashboardProgressEvent e) {
        return new DashboardProgressEvent(
                e.datasetId(),
                e.complete() ? e.stage() : "replay",
                e.complete() ? e.message() : "Last known dashboard progress. Waiting for live updates...",
                e.scannedFiles(),
                e.totalFiles(),
                e.processedRows(),
                e.failedFiles(),
                e.files(),
                e.complete() ? e.charts() : List.of(),
                e.complete() ? e.dashboard() : null,
                e.complete()
        );
    }
}
