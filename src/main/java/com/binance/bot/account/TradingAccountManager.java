package com.binance.bot.account;

import com.binance.bot.config.BinanceProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
@Service
public class TradingAccountManager {
    private final BinanceProperties properties;
    private final AccountTradingRuntimeFactory runtimeFactory;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ConcurrentMap<String, AccountTradingRuntime> runtimes = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, String> initializationErrors = new ConcurrentHashMap<>();

    public TradingAccountManager(BinanceProperties properties, AccountTradingRuntimeFactory runtimeFactory) {
        this.properties = properties;
        this.runtimeFactory = runtimeFactory;
    }

    @PostConstruct
    public void initialize() {
        configuredAccounts().forEach((accountId, credentials) -> {
            AccountTradingRuntime runtime = null;
            try {
                runtime = runtimeFactory.create(credentials);
                AccountTradingRuntime duplicate = runtimes.putIfAbsent(accountId, runtime);
                if (duplicate != null) throw new IllegalStateException("duplicate accountId");
                runtime.initialize();
                log.info("[accountId={} alias={}] 账户运行时初始化完成", accountId, credentials.alias());
            } catch (Exception e) {
                AccountTradingRuntime failed = runtimes.remove(accountId);
                if (failed != null) {
                    try { failed.shutdown(); } catch (Exception ignored) { }
                }
                initializationErrors.put(accountId, safeMessage(e));
                log.error("[accountId={}] 账户运行时初始化失败；其他账号继续运行: {}", accountId, safeMessage(e));
            }
        });
    }

    @PreDestroy
    public void shutdown() {
        runtimes.values().forEach(runtime -> {
            try { runtime.shutdown(); }
            catch (Exception e) { log.error("[accountId={}] 关闭失败: {}", runtime.accountId(), safeMessage(e)); }
        });
    }

    public Optional<AccountTradingRuntime> find(String accountId) {
        return Optional.ofNullable(runtimes.get(accountId));
    }

    public List<AccountTradingRuntime> runtimes() {
        return runtimes.values().stream().sorted(Comparator.comparing(AccountTradingRuntime::accountId)).toList();
    }

    public List<AccountSummary> summaries() {
        List<AccountSummary> result = new ArrayList<>();
        runtimes().forEach(runtime -> result.add(new AccountSummary(runtime.accountId(), runtime.alias(),
                runtime.initialized(), runtime.engine().getIsRunning().get(),
                runtime.engine().getLiveArmed().get(), runtime.engine().getCurrentStatus().get().name(),
                runtime.engine().getSymbol(), runtime.engine().isAccountStreamReady(), null)));
        initializationErrors.forEach((id, error) -> result.add(new AccountSummary(id, id, false, false,
                false, "INITIALIZATION_FAILED", null, false, error)));
        result.sort(Comparator.comparing(AccountSummary::accountId));
        return result;
    }

    public Map<String, OperationResult> startAll() {
        Map<String, OperationResult> results = new LinkedHashMap<>();
        runtimes().forEach(runtime -> {
            try {
                boolean accepted = runtime.start();
                results.put(runtime.accountId(), new OperationResult(accepted,
                        accepted ? "started" : runtime.engine().getStatusReason().get()));
            } catch (Exception e) {
                results.put(runtime.accountId(), new OperationResult(false, safeMessage(e)));
                log.error("[accountId={}] 批量启动失败；继续处理其他账号: {}",
                        runtime.accountId(), safeMessage(e));
            }
        });
        initializationErrors.forEach((id, error) -> results.put(id, new OperationResult(false, error)));
        return results;
    }

    public Map<String, OperationResult> armAll() {
        Map<String, OperationResult> results = new LinkedHashMap<>();
        runtimes().forEach(runtime -> {
            try {
                boolean accepted = runtime.arm();
                results.put(runtime.accountId(), new OperationResult(accepted,
                        accepted ? "LIVE armed" : "LIVE 双开关未配置或账号成交流未就绪"));
            } catch (Exception e) {
                results.put(runtime.accountId(), new OperationResult(false, safeMessage(e)));
                log.error("[accountId={}] 批量解锁 LIVE 失败；继续处理其他账号: {}",
                        runtime.accountId(), safeMessage(e));
            }
        });
        initializationErrors.forEach((id, error) -> results.put(id, new OperationResult(false, error)));
        return results;
    }

    public Map<String, OperationResult> stopAll() {
        Map<String, OperationResult> results = new LinkedHashMap<>();
        runtimes().forEach(runtime -> {
            try {
                boolean clean = runtime.stop();
                results.put(runtime.accountId(), new OperationResult(clean,
                        clean ? "stopped" : runtime.engine().getStatusReason().get()));
            } catch (Exception e) {
                results.put(runtime.accountId(), new OperationResult(false, safeMessage(e)));
                log.error("[accountId={}] 批量停止失败；继续处理其他账号: {}",
                        runtime.accountId(), safeMessage(e));
            }
        });
        initializationErrors.forEach((id, error) -> results.put(id, new OperationResult(false, error)));
        return results;
    }

    private Map<String, AccountCredentials> configuredAccounts() {
        Map<String, AccountCredentials> result = new LinkedHashMap<>();
        Map<String, BinanceProperties.CredentialProfile> configuredProfiles = configuredProfiles();
        boolean profilesConfigured = !configuredProfiles.isEmpty()
                || notBlank(properties.getAccountProfilesJson());
        configuredProfiles.forEach((accountId, profile) -> {
            String errorId = safeProfileId(accountId);
            try {
                if (profile == null) throw new IllegalArgumentException("credential profile is missing");
                if (!profile.isEnabled()) return;
                AccountCredentials credentials = new AccountCredentials(accountId,
                        displayAlias(profile.getAlias(), accountId), profile.getApiKey(), profile.getSecretKey());
                if (credentials.complete()) result.put(accountId, credentials);
                else if (hasAnyCredentialValue(profile)) {
                    initializationErrors.put(errorId, "API credentials are incomplete");
                }
            } catch (Exception e) {
                initializationErrors.put(errorId, safeMessage(e));
                log.error("[accountId={}] 账号配置无效；其他账号继续初始化: {}", errorId, safeMessage(e));
            }
        });
        if (!profilesConfigured && result.isEmpty()
                && complete(properties.getApi().getApiKey(), properties.getApi().getSecretKey())) {
            result.put("default", new AccountCredentials("default",
                    displayAlias(properties.getApi().getApiKeyAlias(), "default"),
                    properties.getApi().getApiKey(), properties.getApi().getSecretKey()));
        }
        if (!profilesConfigured && result.isEmpty() && properties.getStrategy().isObserveMode()) {
            result.put("default", new AccountCredentials("default", "default", "", ""));
        }
        return result;
    }

    private Map<String, BinanceProperties.CredentialProfile> configuredProfiles() {
        Map<String, BinanceProperties.CredentialProfile> result = new LinkedHashMap<>(
                properties.getApi().getProfiles());
        String profilesJson = properties.getAccountProfilesJson();
        if (!notBlank(profilesJson)) return result;
        try {
            JsonNode root = objectMapper.readTree(profilesJson);
            if (root == null || !root.isObject()) throw new IllegalArgumentException("profiles must be an object");
            root.fields().forEachRemaining(entry -> result.put(entry.getKey(),
                    objectMapper.convertValue(entry.getValue(), BinanceProperties.CredentialProfile.class)));
        } catch (Exception e) {
            // Never include parser excerpts because the JSON contains credentials.
            initializationErrors.put("profiles-json", "BINANCE_API_PROFILES_JSON is invalid");
            log.error("BINANCE_API_PROFILES_JSON 无效；拒绝使用 legacy 凭据回退");
            result.clear();
        }
        return result;
    }

    private boolean hasAnyCredentialValue(BinanceProperties.CredentialProfile profile) {
        return notBlank(profile.getApiKey()) || notBlank(profile.getSecretKey()) || notBlank(profile.getAlias());
    }

    private boolean complete(String apiKey, String secretKey) { return notBlank(apiKey) && notBlank(secretKey); }
    private boolean notBlank(String value) { return value != null && !value.isBlank(); }
    private String displayAlias(String alias, String fallback) { return notBlank(alias) ? alias : fallback; }
    private String safeProfileId(String accountId) {
        return accountId == null || accountId.isBlank() ? "invalid-profile" : accountId;
    }
    private String safeMessage(Exception e) {
        String value = e.getMessage();
        return value == null || value.isBlank() ? e.getClass().getSimpleName() : value;
    }

    public record AccountSummary(String accountId, String alias, boolean initialized, boolean running,
                                 boolean liveArmed, String status, String symbol,
                                 boolean accountStreamReady, String error) { }
    public record OperationResult(boolean success, String reason) { }
}
