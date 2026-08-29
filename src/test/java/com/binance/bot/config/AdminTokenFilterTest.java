package com.binance.bot.config;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AdminTokenFilterTest {
    @Test
    void rejectsUnauthenticatedBotControlRequest() throws Exception {
        AdminTokenFilter filter = filter("test-token");
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/bot/start");
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, new MockFilterChain());

        assertEquals(401, response.getStatus());
    }

    @Test
    void acceptsCorrectAdminToken() throws Exception {
        AdminTokenFilter filter = filter("test-token");
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/bot/stop");
        request.addHeader("X-Bot-Admin-Token", "test-token");
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, new MockFilterChain());

        assertEquals(200, response.getStatus());
    }

    private static AdminTokenFilter filter(String token) {
        BinanceProperties properties = new BinanceProperties();
        properties.getSecurity().setAdminToken(token);
        return new AdminTokenFilter(properties);
    }
}
