package com.binance.bot.controller;

import com.binance.bot.strategy.HighFrequencyVolumeChurnEngine;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/bot")
public class BotDashboardController {

    private final HighFrequencyVolumeChurnEngine engine;

    public BotDashboardController(HighFrequencyVolumeChurnEngine engine) {
        this.engine = engine;
    }

    @GetMapping("/status")
    public Map<String, Object> getStatus() {
        return Map.ofEntries(
                Map.entry("running", engine.getIsRunning().get()),
                Map.entry("status", engine.getCurrentStatus().get().name()),
                Map.entry("executionMode", engine.getExecutionMode()),
                Map.entry("minimumPaperObservations", engine.getMinimumPaperObservations()),
                Map.entry("symbol", engine.getSymbol()),
                Map.entry("totalVolumeUsdt", engine.getTotalVolumeUsdt().get()),
                Map.entry("roundTripsCompleted", engine.getRoundTripsCompleted().get()),
                Map.entry("usedApiWeight1m", engine.getUsedApiWeight()),
                Map.entry("marketData", engine.getMarketDataSnapshot()),
                Map.entry("entrySignal", engine.getLastEntryDecision()),
                Map.entry("postFillOutcomes", engine.getPostFillOutcomes()),
                Map.entry("risk", engine.getRiskSnapshot())
        );
    }

    @PostMapping("/start")
    public String startEngine() {
        return engine.startTrading() ? "引擎已启动" : "引擎拒绝启动；请检查 status 的状态与风险日志";
    }

    @PostMapping("/stop")
    public String stopEngine() {
        engine.stopTrading();
        return "纯刷量引擎已停止，已清理所有活跃订单";
    }
}
