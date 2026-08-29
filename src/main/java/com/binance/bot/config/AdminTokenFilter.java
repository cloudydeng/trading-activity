package com.binance.bot.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/** Protects every bot API, including status, before it can be placed behind a public reverse proxy. */
@Component
public class AdminTokenFilter extends OncePerRequestFilter {
    private static final String TOKEN_HEADER = "X-Bot-Admin-Token";
    private final BinanceProperties properties;

    public AdminTokenFilter(BinanceProperties properties) {
        this.properties = properties;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !request.getRequestURI().startsWith("/api/bot/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        String expected = properties.getSecurity().getAdminToken();
        if (expected == null || expected.isBlank()) {
            response.sendError(HttpStatus.SERVICE_UNAVAILABLE.value(), "Bot API admin token is not configured");
            return;
        }
        String provided = request.getHeader(TOKEN_HEADER);
        boolean valid = provided != null && MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.UTF_8), provided.getBytes(StandardCharsets.UTF_8));
        if (!valid) {
            response.sendError(HttpStatus.UNAUTHORIZED.value(), "Invalid bot API admin token");
            return;
        }
        filterChain.doFilter(request, response);
    }
}
