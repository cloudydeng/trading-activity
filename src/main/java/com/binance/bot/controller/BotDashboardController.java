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
                Map.entry("executionMode", engine.getExecutionMode()),
                Map.entry("liveArmed", engine.getLiveArmed().get()),
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
    public String startEngine() {
        return engine.startTrading() ? "引擎已启动" : "引擎拒绝启动；请检查 status 的状态与风险日志";
    }

    @PostMapping("/stop")
    public String stopEngine() {
        engine.disarmLiveTrading();
        return "引擎已停止，LIVE 已解除，已清理活动订单";
    }

    @PostMapping("/live/arm")
    public ResponseEntity<String> armLive(@RequestBody LiveArmRequest request) {
        String expected = properties.getSecurity().getAdminPassword();
        boolean passwordMatches = expected != null && request.password() != null && MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.UTF_8), request.password().getBytes(StandardCharsets.UTF_8));
        if (!passwordMatches || !"ENABLE LIVE".equals(request.confirmation())) return ResponseEntity.status(401).body("二次验证失败");
        return engine.armLiveTrading() ? ResponseEntity.ok("LIVE 已临时解锁；服务重启或停止后自动解除")
                : ResponseEntity.status(409).body("服务器未配置 LIVE 双开关");
    }

    @PostMapping("/live/disarm")
    public String disarmLive() {
        engine.disarmLiveTrading();
        return "LIVE 已解除";
    }

    public record LiveArmRequest(String password, String confirmation) { }
}
