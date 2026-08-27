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
        return Map.of(
                "running", engine.getIsRunning().get(),
                "status", engine.getCurrentStatus().get().name(),
                "symbol", engine.getSymbol(),
                "totalVolumeUsdt", engine.getTotalVolumeUsdt().get(),
                "roundTripsCompleted", engine.getRoundTripsCompleted().get(),
                "usedApiWeight1m", engine.getUsedApiWeight(),
                "entrySignal", engine.getLastEntryDecision(),
                "postFillOutcomes", engine.getPostFillOutcomes()
        );
    }

    @PostMapping("/start")
    public String startEngine() {
        engine.startTrading();
        return "纯刷量引擎已启动";
    }

    @PostMapping("/stop")
    public String stopEngine() {
        engine.stopTrading();
        return "纯刷量引擎已停止，已清理所有活跃订单";
    }
}
