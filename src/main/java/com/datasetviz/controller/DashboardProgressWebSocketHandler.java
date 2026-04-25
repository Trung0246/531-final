package com.datasetviz.controller;

import com.datasetviz.service.DashboardProgressService;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;

@Component
public class DashboardProgressWebSocketHandler extends TextWebSocketHandler {

    private final DashboardProgressService dashboardProgressService;

    public DashboardProgressWebSocketHandler(DashboardProgressService dashboardProgressService) {
        this.dashboardProgressService = dashboardProgressService;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        String datasetId = datasetId(session.getUri());
        if (datasetId == null || datasetId.isBlank()) {
            session.close(CloseStatus.BAD_DATA.withReason("Missing datasetId query parameter"));
            return;
        }

        session.getAttributes().put("datasetId", datasetId);
        dashboardProgressService.subscribe(datasetId, session);
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        if ("ping".equalsIgnoreCase(message.getPayload())) {
            session.sendMessage(new TextMessage("pong"));
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        dashboardProgressService.unsubscribe(session);
    }

    private String datasetId(URI uri) {
        if (uri == null) {
            return null;
        }
        return UriComponentsBuilder.fromUri(uri).build().getQueryParams().getFirst("datasetId");
    }
}
