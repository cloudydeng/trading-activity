package com.binance.bot.config;

import com.binance.bot.strategy.DailyTradeStatsStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/** Holds immutable API-key/secret pairs so a request can never mix two profiles during a switch. */
@Slf4j
@Component
public class BinanceCredentialManager {
    private final BinanceProperties properties;
    private final Map<String, CredentialSnapshot> profiles;
    private final AtomicReference<CredentialSnapshot> current;

    public BinanceCredentialManager(BinanceProperties properties, DailyTradeStatsStore dailyStatsStore) {
        this.properties = properties;
        LinkedHashMap<String, CredentialSnapshot> loaded = new LinkedHashMap<>();
        addProfile(loaded, properties.getApi().getApiKeyAlias(), properties.getApi().getApiKey(),
                properties.getApi().getSecretKey(), "primary");
        for (Map.Entry<String, BinanceProperties.CredentialProfile> entry
                : properties.getApi().getProfiles().entrySet()) {
            BinanceProperties.CredentialProfile profile = entry.getValue();
            if (profile == null) continue;
            addProfile(loaded, profile.getAlias(), profile.getApiKey(), profile.getSecretKey(), entry.getKey());
        }
        this.profiles = Collections.unmodifiableMap(new LinkedHashMap<>(loaded));

        CredentialSnapshot initial = loaded.values().stream().findFirst().orElseGet(() ->
                new CredentialSnapshot(normalizeAlias(properties.getApi().getApiKeyAlias()), "", ""));
        String persisted = dailyStatsStore.loadActiveApiAlias().orElse(null);
        if (persisted != null && loaded.containsKey(persisted)) {
            initial = loaded.get(persisted);
        } else if (persisted != null) {
            log.warn("持久化 API 别名 {} 当前未配置，继续使用默认凭据", persisted);
        }
        current = new AtomicReference<>(initial);
        mirrorToProperties(initial);
        log.info("已加载 {} 个 Binance API 凭据配置；当前别名: {}", loaded.size(), initial.alias());
    }

    private void addProfile(Map<String, CredentialSnapshot> target, String alias, String apiKey,
                            String secretKey, String sourceName) {
        boolean any = !isBlank(alias) || !isBlank(apiKey) || !isBlank(secretKey);
        boolean complete = !isBlank(alias) && !isBlank(apiKey) && !isBlank(secretKey);
        if (!any) return;
        if (!complete) {
            // The legacy primary defaults alias to "unlabeled" even when OBSERVE has no credentials.
            if (isBlank(apiKey) && isBlank(secretKey) && "primary".equals(sourceName)) return;
            throw new IllegalStateException("Binance API 凭据配置不完整: " + sourceName);
        }
        String normalizedAlias = normalizeAlias(alias);
        if (!normalizedAlias.matches("[A-Za-z0-9._-]{1,64}")) {
            throw new IllegalStateException("Binance API 别名格式无效: " + sourceName);
        }
        CredentialSnapshot previous = target.putIfAbsent(normalizedAlias,
                new CredentialSnapshot(normalizedAlias, apiKey, secretKey));
        if (previous != null) throw new IllegalStateException("Binance API 别名重复: " + normalizedAlias);
    }

    public CredentialSnapshot current() { return current.get(); }
    public String currentAlias() { return current.get().alias(); }

    public List<ProfileView> profileViews() {
        String active = currentAlias();
        List<ProfileView> result = new ArrayList<>();
        for (String alias : profiles.keySet()) result.add(new ProfileView(alias, alias.equals(active)));
        return List.copyOf(result);
    }

    public synchronized CredentialSnapshot activate(String alias) {
        CredentialSnapshot target = profiles.get(normalizeAlias(alias));
        if (target == null) throw new IllegalArgumentException("未配置 API 别名: " + alias);
        current.set(target);
        mirrorToProperties(target);
        return target;
    }

    public synchronized void restore(CredentialSnapshot snapshot) {
        if (snapshot == null || !profiles.containsKey(snapshot.alias())) {
            throw new IllegalArgumentException("无法恢复未知 API 凭据");
        }
        current.set(snapshot);
        mirrorToProperties(snapshot);
    }

    public boolean contains(String alias) {
        return alias != null && profiles.containsKey(alias.trim());
    }

    private void mirrorToProperties(CredentialSnapshot snapshot) {
        properties.getApi().setApiKeyAlias(snapshot.alias());
        properties.getApi().setApiKey(snapshot.apiKey());
        properties.getApi().setSecretKey(snapshot.secretKey());
    }

    private String normalizeAlias(String alias) {
        return alias == null || alias.isBlank() ? "unlabeled" : alias.trim();
    }

    private boolean isBlank(String value) { return value == null || value.isBlank(); }

    public record ProfileView(String alias, boolean active) { }

    public static final class CredentialSnapshot {
        private final String alias;
        private final String apiKey;
        private final String secretKey;

        public CredentialSnapshot(String alias, String apiKey, String secretKey) {
            this.alias = alias;
            this.apiKey = apiKey;
            this.secretKey = secretKey;
        }

        public String alias() { return alias; }
        public String apiKey() { return apiKey; }
        public String secretKey() { return secretKey; }

        @Override public String toString() { return "CredentialSnapshot[alias=" + alias + ", redacted]"; }
    }
}
