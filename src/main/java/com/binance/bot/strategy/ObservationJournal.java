package com.binance.bot.strategy;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReentrantLock;

/** Appends completed post-fill market outcomes as JSON Lines for optional diagnostics. */
@Slf4j
public class ObservationJournal {
    private final Path outputFile;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ReentrantLock lock = new ReentrantLock();

    public ObservationJournal(String outputFile) {
        this.outputFile = Path.of(outputFile);
    }

    public void append(PostFillOutcomeTracker.Outcome outcome) {
        lock.lock();
        try {
            try {
                Path parent = outputFile.getParent();
                if (parent != null) Files.createDirectories(parent);
                Files.writeString(outputFile, objectMapper.writeValueAsString(outcome) + System.lineSeparator(),
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                log.error("无法持久化观测结果到 {}", outputFile, e);
            }
        } finally {
            lock.unlock();
        }
    }
}
