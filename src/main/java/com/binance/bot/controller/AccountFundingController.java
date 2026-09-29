package com.binance.bot.controller;

import com.binance.bot.service.AccountFundingHistoryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AccountFundingController {
    private final AccountFundingHistoryService fundingHistory;

    public AccountFundingController(AccountFundingHistoryService fundingHistory) {
        this.fundingHistory = fundingHistory;
    }

    @GetMapping("/api/accounts/funding")
    public AccountFundingHistoryService.Snapshot funding() {
        return fundingHistory.snapshot();
    }
}
