package com.binance.bot.controller;

import com.binance.bot.config.BinanceProperties;
import com.binance.bot.strategy.HighFrequencyVolumeChurnEngine;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BotDashboardControllerTest {

    @Test
    void authenticatedCallerCanArmLiveWithoutSecondaryPayload() {
        HighFrequencyVolumeChurnEngine engine = mock(HighFrequencyVolumeChurnEngine.class);
        when(engine.armLiveTrading()).thenReturn(true);
        BotDashboardController controller = new BotDashboardController(engine, new BinanceProperties());

        var response = controller.armLive();

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals("LIVE 已临时解锁；服务重启或停止后自动解除", response.getBody());
        verify(engine).armLiveTrading();
    }
}
