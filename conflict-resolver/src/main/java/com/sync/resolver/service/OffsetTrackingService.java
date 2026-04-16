package com.sync.resolver.service;

import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class OffsetTrackingService {
    private final Map<String, Long> processedOffsets = new ConcurrentHashMap<>();

    public void recordProcessedOffset(String topicPartition, long offset) {
        processedOffsets.put(topicPartition, offset);
    }

    public Long getProcessedOffset(String topicPartition) {
        return processedOffsets.get(topicPartition);
    }

    public boolean isAlreadyProcessed(String topicPartition, long offset) {
        return processedOffsets.containsKey(topicPartition) && processedOffsets.get(topicPartition) >= offset;
    }
}
