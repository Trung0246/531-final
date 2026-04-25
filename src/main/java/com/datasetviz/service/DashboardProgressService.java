package com.datasetviz.service;

import com.datasetviz.dto.DashboardProgressEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Service
public class DashboardProgressService {

    private final ObjectMapper objectMapper;
    private final ConcurrentMap<String, Set<WebSocketSession>> sessionsByDataset = new ConcurrentHashMap<>();

    public DashboardProgressService() {
        this(new ObjectMapper());
    }

    public DashboardProgressService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public void subscribe(String datasetId, WebSocketSession session) {
        sessionsByDataset.computeIfAbsent(datasetId, ignored -> ConcurrentHashMap.newKeySet()).add(session);
        publishToSession(session, new DashboardProgressEvent(datasetId, "connected", "Live dashboard progress connected.", 0, 0, 0, 0, false));
    }

    public void unsubscribe(WebSocketSession session) {
        sessionsByDataset.values().forEach(sessions -> sessions.remove(session));
    }

    public void publish(DashboardProgressEvent event) {
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
}
