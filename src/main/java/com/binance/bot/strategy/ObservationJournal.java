package com.binance.bot.strategy;

import com.binance.bot.config.BinanceProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Appends completed paper observations as JSON Lines so a restart never discards the research sample. */
@Slf4j
@Component
public class ObservationJournal {
    private final Path outputFile;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ObservationJournal(BinanceProperties properties) {
        this.outputFile = Path.of(properties.getStrategy().getObservationOutputFile());
    }

    public synchronized void append(PostFillOutcomeTracker.Outcome outcome) {
        try {
            Path parent = outputFile.getParent();
            if (parent != null) Files.createDirectories(parent);
            Files.writeString(outputFile, objectMapper.writeValueAsString(outcome) + System.lineSeparator(),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            log.error("无法持久化观测结果到 {}", outputFile, e);
        }
    }
}
