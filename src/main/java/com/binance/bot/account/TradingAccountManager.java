package com.binance.bot.account;

import com.binance.bot.config.BinanceProperties;
import com.binance.bot.strategy.DailyTradeStatsStore;
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
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@Service
public class TradingAccountManager {
    private final BinanceProperties properties;
    private final AccountTradingRuntimeFactory runtimeFactory;
    private final DailyTradeStatsStore dailyStatsStore;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ConcurrentMap<String, AccountTradingRuntime> runtimes = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, String> initializationErrors = new ConcurrentHashMap<>();
    private final ReentrantLock lock = new ReentrantLock();

    public TradingAccountManager(BinanceProperties properties, AccountTradingRuntimeFactory runtimeFactory,
                                 DailyTradeStatsStore dailyStatsStore) {
        this.properties = properties;
        this.runtimeFactory = runtimeFactory;
        this.dailyStatsStore = dailyStatsStore;
    }

    @PostConstruct
    public void initialize() {
        lock.lock();
        try {
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
        } finally {
            lock.unlock();
        }
    }

    /**
     * Re-reads the protected environment file and creates only account runtimes that are not
     * already present. Existing runtimes are deliberately never replaced or stopped: a reload
     * must not disturb an active SELL order or an account's LIVE state.
     */
    public ReloadResult reloadProfiles() {
        lock.lock();
        try {
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
                    log.info("[accountId={} alias={}] 热加载账户运行时完成（默认停止）",
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
        } finally {
            lock.unlock();
        }
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
                        profile.getOrderAmountsUsdt(), profile.getSymbolStrategies(), profile.getSymbols());
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

    public Optional<AccountSymbolsConfiguration> accountSymbolsConfiguration(String accountId) {
        AccountTradingRuntime runtime = runtimes.get(accountId);
        if (runtime == null) return Optional.empty();
        List<String> activeSymbols = runtime.engines().stream()
                .map(engine -> engine.getSymbol().toUpperCase()).toList();
        List<String> configuredSymbols = dailyStatsStore.loadAccountSymbols(accountId).orElse(activeSymbols);
        boolean editable = runtime.canChangeConfiguredSymbols();
        return Optional.of(new AccountSymbolsConfiguration(runtime.accountId(), runtime.alias(),
                configuredSymbols, activeSymbols, !configuredSymbols.equals(activeSymbols), editable,
                editable ? "" : "请先停止该账户的全部币种，并确认没有活动订单"));
    }

    public SymbolsUpdateResult updateAccountSymbols(String accountId, List<String> symbols) {
        lock.lock();
        try {
            AccountTradingRuntime runtime = runtimes.get(accountId);
            if (runtime == null) return new SymbolsUpdateResult(false, "账户不存在或未初始化", null);
            if (!runtime.canChangeConfiguredSymbols()) {
                return new SymbolsUpdateResult(false, "请先停止该账户的全部币种，并确认没有活动订单",
                        accountSymbolsConfiguration(accountId).orElse(null));
            }
            try {
                dailyStatsStore.saveAccountSymbols(accountId, symbols);
                List<String> configuredSymbols = dailyStatsStore.loadAccountSymbols(accountId).orElse(symbols);
                HotApplySymbolsResult hotApply = hotApplyAccountSymbols(runtime, configuredSymbols);
                AccountSymbolsConfiguration configuration = accountSymbolsConfiguration(accountId).orElseThrow();
                String message = hotApply.applied()
                        ? "交易对配置已保存并热应用"
                        : "交易对配置已保存到 SQLite，" + hotApply.message() + "；需重启服务生效";
                return new SymbolsUpdateResult(true, message, configuration);
            } catch (IllegalArgumentException e) {
                return new SymbolsUpdateResult(false, safeMessage(e), safeAccountSymbolsConfiguration(accountId));
            } catch (RuntimeException e) {
                log.error("[accountId={}] 保存账户交易对配置失败: {}", safeProfileId(accountId), safeMessage(e));
                return new SymbolsUpdateResult(false, "保存交易对配置失败", safeAccountSymbolsConfiguration(accountId));
            }
        } finally {
            lock.unlock();
        }
    }

    private HotApplySymbolsResult hotApplyAccountSymbols(AccountTradingRuntime runtime, List<String> configuredSymbols) {
        if (!runtime.canChangeConfiguredSymbols()) {
            return new HotApplySymbolsResult(false, "当前账户状态已变化，无法安全热应用");
        }
        Map<String, AccountTradingRuntimeFactory.AccountSymbolRuntime> additions = new LinkedHashMap<>();
        boolean applied = false;
        try {
            for (String symbol : configuredSymbols) {
                String normalized = symbol == null ? "" : symbol.trim().toUpperCase();
                if (normalized.isBlank() || runtime.engine(normalized).isPresent()) continue;
                AccountTradingRuntimeFactory.AccountSymbolRuntime addition = runtimeFactory.createSymbolRuntime(
                        runtime.credentials(), runtime.tradeClient(), runtime.userDataStream(),
                        runtime.accountRiskCoordinator(), normalized);
                addition.engine().initialize();
                additions.put(normalized, addition);
            }
            AccountTradingRuntime.ApplySymbolsResult result =
                    runtime.applyConfiguredSymbols(configuredSymbols, additions);
            applied = result.applied();
            return new HotApplySymbolsResult(result.applied(), result.message());
        } catch (RuntimeException e) {
            log.error("[accountId={}] 交易对配置热应用失败，已保留 SQLite 配置等待重启生效: {}",
                    runtime.accountId(), safeMessage(e));
            return new HotApplySymbolsResult(false, "热应用失败: " + safeMessage(e));
        } finally {
            if (!applied) {
                additions.values().forEach(addition -> {
                    try { addition.engine().shutdown(); } catch (RuntimeException ignored) { }
                });
            }
        }
    }

    private AccountSymbolsConfiguration safeAccountSymbolsConfiguration(String accountId) {
        try {
            return accountSymbolsConfiguration(accountId).orElse(null);
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    public List<AccountSummary> summaries() {
        List<AccountSummary> result = new ArrayList<>();
        runtimes().forEach(runtime -> runtime.engines().forEach(engine -> result.add(new AccountSummary(
                runtime.accountId() + "::" + engine.getSymbol(), runtime.accountId(), runtime.alias(),
                runtime.symbolCount(), runtime.initialized(), engine.getIsRunning().get(),
                engine.getCurrentStatus().get().name(), engine.getStrategyMode(), engine.getSymbol(),
                engine.isAccountStreamReady(), null))));
        initializationErrors.forEach((id, error) -> result.add(new AccountSummary(id, id, id, 0,
                false, false, "INITIALIZATION_FAILED", null, null, false, error)));
        result.sort(Comparator.comparing(AccountSummary::accountId)
                .thenComparing(summary -> Optional.ofNullable(summary.symbol()).orElse("")));
        return result;
    }

    public Map<String, OperationResult> startAll() {
        Map<String, OperationResult> results = new LinkedHashMap<>();
        runtimes().forEach(runtime -> runtime.engines().forEach(engine -> {
            String key = operationKey(runtime, engine.getSymbol());
            try {
                boolean accepted = runtime.start(engine.getSymbol());
                results.put(key, new OperationResult(accepted,
                        accepted ? "started" : engine.getStatusReason().get()));
            } catch (Exception e) {
                results.put(key, new OperationResult(false, safeMessage(e)));
                log.error("[accountId={} symbol={}] 批量启动失败；继续处理其他实例: {}",
                        runtime.accountId(), engine.getSymbol(), safeMessage(e));
            }
        }));
        initializationErrors.forEach((id, error) -> results.put(id, new OperationResult(false, error)));
        return results;
    }

    public Map<String, OperationResult> stopAll() {
        Map<String, OperationResult> results = new LinkedHashMap<>();
        runtimes().forEach(runtime -> runtime.engines().forEach(engine -> {
            String key = operationKey(runtime, engine.getSymbol());
            try {
                boolean clean = runtime.stop(engine.getSymbol());
                results.put(key, new OperationResult(clean,
                        clean ? "stopped" : engine.getStatusReason().get()));
            } catch (Exception e) {
                results.put(key, new OperationResult(false, safeMessage(e)));
                log.error("[accountId={} symbol={}] 批量停止失败；继续处理其他实例: {}",
                        runtime.accountId(), engine.getSymbol(), safeMessage(e));
            }
        }));
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
                        profile.getOrderAmountsUsdt(), profile.getSymbolStrategies(), profile.getSymbols());
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

    private String operationKey(AccountTradingRuntime runtime, String symbol) {
        return runtime.symbolCount() == 1 ? runtime.accountId() : runtime.accountId() + "/" + symbol;
    }

    public record AccountSummary(String runtimeId, String accountId, String alias, int symbolCount,
                                 boolean initialized, boolean running,
                                 String status, String strategyMode, String symbol,
                                 boolean accountStreamReady, String error) { }
    public record OperationResult(boolean success, String reason) { }
    public record ReloadResult(int added, List<String> addedAccounts, Map<String, String> errors) { }
    public record AccountSymbolsConfiguration(String accountId, String accountAlias,
                                              List<String> configuredSymbols, List<String> activeSymbols,
                                              boolean restartRequired, boolean editable,
                                              String editBlockReason) { }
    public record SymbolsUpdateResult(boolean accepted, String message,
                                      AccountSymbolsConfiguration configuration) { }
    private record HotApplySymbolsResult(boolean applied, String message) { }
}
