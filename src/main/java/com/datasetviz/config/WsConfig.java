package com.datasetviz.config;

import com.datasetviz.controller.ProgressWsHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
public class WsConfig implements WebSocketConfigurer {

    private final ProgressWsHandler progressWsHandler;

    public WsConfig(ProgressWsHandler progressWsHandler) {
        this.progressWsHandler = progressWsHandler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(progressWsHandler, "/ws/dashboard-progress")
                .setAllowedOrigins("*");
    }
}
