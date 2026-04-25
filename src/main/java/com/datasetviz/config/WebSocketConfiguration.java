package com.datasetviz.config;

import com.datasetviz.controller.DashboardProgressWebSocketHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
public class WebSocketConfiguration implements WebSocketConfigurer {

    private final DashboardProgressWebSocketHandler dashboardProgressWebSocketHandler;

    public WebSocketConfiguration(DashboardProgressWebSocketHandler dashboardProgressWebSocketHandler) {
        this.dashboardProgressWebSocketHandler = dashboardProgressWebSocketHandler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(dashboardProgressWebSocketHandler, "/ws/dashboard-progress")
                .setAllowedOrigins("*");
    }
}
