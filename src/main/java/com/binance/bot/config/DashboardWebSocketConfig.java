package com.binance.bot.config;

import com.binance.bot.notification.RecentFillWebSocketHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
public class DashboardWebSocketConfig implements WebSocketConfigurer {
    private final RecentFillWebSocketHandler recentFillHandler;

    public DashboardWebSocketConfig(RecentFillWebSocketHandler recentFillHandler) {
        this.recentFillHandler = recentFillHandler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(recentFillHandler, "/api/accounts/notifications/ws");
    }
}
