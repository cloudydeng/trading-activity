package com.binance.bot.controller;

import com.binance.bot.strategy.HighFrequencyVolumeChurnEngine;
import com.binance.bot.config.BinanceProperties;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import java.util.Map;

@RestController
@RequestMapping("/api/bot")
public class BotDashboardController {

    private final HighFrequencyVolumeChurnEngine engine;
    private final BinanceProperties properties;

    public BotDashboardController(HighFrequencyVolumeChurnEngine engine, BinanceProperties properties) {
        this.engine = engine;
        this.properties = properties;
    }

    @GetMapping("/status")
    public Map<String, Object> getStatus() {
        return Map.ofEntries(
                Map.entry("running", engine.getIsRunning().get()),
                Map.entry("status", engine.getCurrentStatus().get().name()),
                Map.entry("statusReason", engine.getStatusReason().get()),
                Map.entry("executionMode", engine.getExecutionMode()),
                Map.entry("liveArmed", engine.getLiveArmed().get()),
                Map.entry("accountStreamReady", engine.isAccountStreamReady()),
                Map.entry("minimumPaperObservations", engine.getMinimumPaperObservations()),
                Map.entry("symbol", engine.getSymbol()),
                Map.entry("totalVolumeUsdt", engine.getTotalVolumeUsdt().get()),
                Map.entry("roundTripsCompleted", engine.getRoundTripsCompleted().get()),
                Map.entry("usedApiWeight1m", engine.getUsedApiWeight()),
                Map.entry("marketData", engine.getMarketDataSnapshot()),
                Map.entry("entrySignal", engine.getLastEntryDecision()),
                Map.entry("marketBaseline", engine.getBaselineOutcomes()),
                Map.entry("qualifiedSignals", engine.getQualifiedSignalOutcomes()),
                Map.entry("risk", engine.getRiskSnapshot())
        );
    }

    @PostMapping("/start")
    public ResponseEntity<String> startEngine() {
        return engine.startTrading() ? ResponseEntity.ok("引擎已启动")
                : ResponseEntity.status(409).body("引擎拒绝启动；请查看控制台的当前状态说明");
    }

    @PostMapping("/stop")
    public ResponseEntity<String> stopEngine() {
        boolean clean = engine.disarmLiveTrading();
        return clean ? ResponseEntity.ok("引擎已停止，LIVE 已解除，交易所确认无活动订单和标的持仓")
                : ResponseEntity.status(409).body("LIVE 已解除，但未能确认安全空仓；系统已保护停机，请人工对账");
    }

    @PostMapping("/live/arm")
    public ResponseEntity<String> armLive() {
        // The authenticated dashboard session or admin-token filter remains the authorization
        // boundary. LIVE arming intentionally has no additional password/confirmation prompt.
        return engine.armLiveTrading() ? ResponseEntity.ok("LIVE 已临时解锁；服务重启或停止后自动解除")
                : ResponseEntity.status(409).body("无法解锁：LIVE 双开关未配置或账户成交流未就绪");
    }

    @PostMapping("/live/disarm")
    public ResponseEntity<String> disarmLive() {
        boolean clean = engine.disarmLiveTrading();
        return clean ? ResponseEntity.ok("LIVE 已解除，交易所状态已确认")
                : ResponseEntity.status(409).body("LIVE 已解除，但订单或持仓仍需人工核对");
    }

    @PostMapping("/liquidate")
    public ResponseEntity<HighFrequencyVolumeChurnEngine.LiquidationResult> liquidate(
            @RequestBody LiquidationRequest request) {
        if (!passwordMatches(request.password()) || !"SELL ALL BASE ASSET".equals(request.confirmation())) {
            return ResponseEntity.status(401).body(new HighFrequencyVolumeChurnEngine.LiquidationResult(
                    false, null, java.math.BigDecimal.ZERO, "二次验证失败"));
        }
        HighFrequencyVolumeChurnEngine.LiquidationResult result = engine.liquidateExistingPosition();
        return result.accepted() ? ResponseEntity.ok(result) : ResponseEntity.status(409).body(result);
    }

    private boolean passwordMatches(String provided) {
        String expected = properties.getSecurity().getAdminPassword();
        return expected != null && provided != null && MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.UTF_8), provided.getBytes(StandardCharsets.UTF_8));
    }

    public record LiquidationRequest(String password, String confirmation) { }
}
