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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
    public synchronized void initialize() {
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

    /**
     * Re-reads the protected environment file and creates only account runtimes that are not
     * already present. Existing runtimes are deliberately never replaced or stopped: a reload
     * must not disturb an active SELL order or an account's LIVE state.
     */
    public synchronized ReloadResult reloadProfiles() {
        Map<String, BinanceProperties.CredentialProfile> profiles = profilesFromEnvironmentFile();
        if (profiles == null) return new ReloadResult(0, List.of(), Map.of("profiles-json", "无法读取或解析账户配置"));
        Map<String, AccountCredentials> candidates = credentialsFromProfiles(profiles);
        List<String> added = new ArrayList<>();
        Map<String, String> errors = new LinkedHashMap<>();
        profiles.forEach((accountId, profile) -> {
            if (profile == null || !profile.isEnabled() || runtimes.containsKey(accountId)) return;
            if (!candidates.containsKey(accountId)) {
                errors.put(safeProfileId(accountId), "API credentials are incomplete or invalid");
                return;
            }
            AccountTradingRuntime runtime = null;
            try {
                runtime = runtimeFactory.create(candidates.get(accountId));
                AccountTradingRuntime duplicate = runtimes.putIfAbsent(accountId, runtime);
                if (duplicate != null) {
                    runtime.shutdown();
                    return;
                }
                runtime.initialize();
                added.add(accountId);
                initializationErrors.remove(accountId);
                log.info("[accountId={} alias={}] 热加载账户运行时完成（默认停止且 LIVE 锁定）",
                        accountId, candidates.get(accountId).alias());
            } catch (Exception e) {
                if (runtime != null) runtimes.remove(accountId, runtime);
                if (runtime != null) try { runtime.shutdown(); } catch (Exception ignored) { }
                String message = safeMessage(e);
                errors.put(safeProfileId(accountId), message);
                initializationErrors.put(safeProfileId(accountId), message);
                log.error("[accountId={}] 热加载账户失败；不影响已有账户: {}", safeProfileId(accountId), message);
            }
        });
        return new ReloadResult(added.size(), List.copyOf(added), Map.copyOf(errors));
    }

    private Map<String, BinanceProperties.CredentialProfile> profilesFromEnvironmentFile() {
        String configuredPath = properties.getAccountProfilesEnvFile();
        if (configuredPath == null || configuredPath.isBlank()) return Map.of();
        try {
            String json = readEnvironmentAssignment(Path.of(configuredPath), "BOT_ACCOUNT_PROFILES_JSON");
            if (json == null || json.isBlank()) return Map.of();
            JsonNode root = objectMapper.readTree(json);
            if (root == null || !root.isObject()) return null;
            Map<String, BinanceProperties.CredentialProfile> result = new LinkedHashMap<>();
            root.fields().forEachRemaining(entry ->
                    result.put(entry.getKey(), objectMapper.convertValue(entry.getValue(),
                            BinanceProperties.CredentialProfile.class)));
            return result;
        } catch (Exception e) {
            log.error("账户环境文件无效，热加载已拒绝（不输出文件内容）: {}", safeMessage(e));
            return null;
        }
    }

    private String readEnvironmentAssignment(Path file, String key) throws Exception {
        if (!Files.isRegularFile(file)) return null;
        String prefix = key + "=";
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if (!line.startsWith(prefix)) continue;
            String value = line.substring(prefix.length()).trim();
            if (value.length() >= 2 && ((value.startsWith("'") && value.endsWith("'"))
                    || (value.startsWith("\"") && value.endsWith("\"")))) {
                return value.substring(1, value.length() - 1);
            }
            return value;
        }
        return null;
    }

    private Map<String, AccountCredentials> credentialsFromProfiles(
            Map<String, BinanceProperties.CredentialProfile> profiles) {
        Map<String, AccountCredentials> result = new LinkedHashMap<>();
        profiles.forEach((accountId, profile) -> {
            try {
                if (profile == null || !profile.isEnabled()) return;
                AccountCredentials credentials = new AccountCredentials(accountId,
                        displayAlias(profile.getAlias(), accountId), profile.getApiKey(), profile.getSecretKey(),
                        profile.getOrderAmountsUsdt(), profile.getSymbolStrategies());
                if (credentials.complete()) result.put(accountId, credentials);
            } catch (Exception ignored) {
                // Caller returns a redacted validation error for this profile.
            }
        });
        return result;
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
                        displayAlias(profile.getAlias(), accountId), profile.getApiKey(), profile.getSecretKey(),
                        profile.getOrderAmountsUsdt(), profile.getSymbolStrategies());
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
            initializationErrors.put("profiles-json", "BOT_ACCOUNT_PROFILES_JSON is invalid");
            log.error("BOT_ACCOUNT_PROFILES_JSON 无效；拒绝使用 legacy 凭据回退");
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
    public record ReloadResult(int added, List<String> addedAccounts, Map<String, String> errors) { }
}
