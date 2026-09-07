package com.binance.bot;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;

/** Loads simple KEY=value assignments from the protected server env file before Spring binds configuration. */
public final class ServerEnvironmentFileLoader {
    private static final String DEFAULT_FILE = "/etc/trading-activity.env";
    private static final Pattern KEY = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private ServerEnvironmentFileLoader() { }

    public static void loadAtStartup() {
        String configuredPath = System.getenv().getOrDefault("BOT_ACCOUNT_PROFILES_ENV_FILE", DEFAULT_FILE);
        if (configuredPath == null || configuredPath.isBlank()) return;
        try {
            load(Path.of(configuredPath));
        } catch (RuntimeException ignored) {
            // Invalid optional configuration path is handled by normal Spring validation.
        }
    }

    static void load(Path file) {
        if (!Files.isRegularFile(file)) return;
        try {
            for (String line : Files.readAllLines(file)) {
                Assignment assignment = parse(line);
                if (assignment == null) continue;
                if (!assignment.key().startsWith("BOT_") && !assignment.key().startsWith("BINANCE_")) continue;
                // A real process environment variable remains authoritative over the file.
                if (System.getenv(assignment.key()) == null && System.getProperty(assignment.key()) == null) {
                    System.setProperty(assignment.key(), assignment.value());
                }
            }
        } catch (IOException ignored) {
            // Missing/unreadable optional configuration is handled by normal Spring validation.
        }
    }

    private static Assignment parse(String line) {
        String value = line == null ? "" : line.trim();
        if (value.isEmpty() || value.startsWith("#")) return null;
        if (value.startsWith("export ")) value = value.substring("export ".length()).trim();
        int separator = value.indexOf('=');
        if (separator <= 0) return null;
        String key = value.substring(0, separator).trim();
        if (!KEY.matcher(key).matches()) return null;
        String rawValue = value.substring(separator + 1).trim();
        return new Assignment(key, unquote(rawValue));
    }

    private static String unquote(String value) {
        if (value.length() >= 2) {
            char first = value.charAt(0);
            char last = value.charAt(value.length() - 1);
            if ((first == '\'' && last == '\'') || (first == '"' && last == '"')) {
                return value.substring(1, value.length() - 1);
            }
        }
        return value;
    }

    private record Assignment(String key, String value) { }
}
