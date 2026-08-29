package com.binance.bot.controller;

import com.binance.bot.config.AdminTokenFilter;
import com.binance.bot.config.BinanceProperties;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

@Controller
public class AdminWebController {
    private final BinanceProperties properties;

    public AdminWebController(BinanceProperties properties) {
        this.properties = properties;
    }

    @GetMapping("/")
    public String home(HttpServletRequest request) {
        HttpSession session = request.getSession(false);
        return session != null && Boolean.TRUE.equals(session.getAttribute(AdminTokenFilter.AUTHENTICATED_SESSION))
                ? "redirect:/dashboard.html" : "redirect:/login.html";
    }

    @PostMapping("/login")
    public String login(@RequestParam String password, HttpServletRequest request) {
        String expected = properties.getSecurity().getAdminPassword();
        if (expected == null || expected.isBlank() || !MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.UTF_8), password.getBytes(StandardCharsets.UTF_8))) {
            return "redirect:/login.html?error=1";
        }
        HttpSession session = request.getSession(true);
        session.setAttribute(AdminTokenFilter.AUTHENTICATED_SESSION, true);
        session.setMaxInactiveInterval(60 * 60);
        return "redirect:/dashboard.html";
    }

    @PostMapping("/logout")
    public String logout(HttpServletRequest request) {
        HttpSession session = request.getSession(false);
        if (session != null) session.invalidate();
        return "redirect:/login.html";
    }
}
