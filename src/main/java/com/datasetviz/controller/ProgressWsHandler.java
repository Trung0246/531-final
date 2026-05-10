package com.datasetviz.controller;

import com.datasetviz.service.ProgressService;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;

@Component
public class ProgressWsHandler extends TextWebSocketHandler {

    private final ProgressService progressService;

    public ProgressWsHandler(ProgressService progressService) {
        this.progressService = progressService;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        String datasetId = datasetId(session.getUri());
        if (datasetId == null || datasetId.isBlank()) {
            session.close(CloseStatus.BAD_DATA.withReason("Missing datasetId query parameter"));
            return;
        }

        session.getAttributes().put("datasetId", datasetId);
        progressService.subscribe(datasetId, session);
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        if ("ping".equalsIgnoreCase(message.getPayload())) {
            session.sendMessage(new TextMessage("pong"));
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        progressService.unsubscribe(session);
    }

    private String datasetId(URI uri) {
        if (uri == null) {
            return null;
        }
        return UriComponentsBuilder.fromUri(uri).build().getQueryParams().getFirst("datasetId");
    }
}
