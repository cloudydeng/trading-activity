package com.binance.bot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class BinanceBotApplication {
    public static void main(String[] args) {
        SpringApplication.run(BinanceBotApplication.class, args);
    }
}
