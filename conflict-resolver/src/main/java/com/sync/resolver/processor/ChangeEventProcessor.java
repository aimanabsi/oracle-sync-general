package com.sync.resolver.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sync.resolver.model.ChangeEvent;
import com.sync.resolver.model.ConflictRecord;
import com.sync.resolver.service.ConflictResolverService;
import com.sync.resolver.service.OffsetTrackingService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class ChangeEventProcessor {

    private final ConflictResolverService conflictResolverService;
    private final OffsetTrackingService offsetTrackingService;
    private final KafkaTemplate<String, ChangeEvent> kafkaTemplate;
    private final ObjectMapper objectMapper; // For converting Kafka message value to ChangeEvent

    @Value("${kafka.resolved-topic-prefix:oracle-sync.resolved.}")
    private String resolvedTopicPrefix;

    // A simple in-memory store for events to detect conflicts. In a real system, this would be a persistent state store.
    private final Map<String, ChangeEvent> latestEvents = new ConcurrentHashMap<>();

    public ChangeEventProcessor(ConflictResolverService conflictResolverService, OffsetTrackingService offsetTrackingService, KafkaTemplate<String, ChangeEvent> kafkaTemplate, ObjectMapper objectMapper) {
        this.conflictResolverService = conflictResolverService;
        this.offsetTrackingService = offsetTrackingService;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${kafka.topics}", groupId = "${kafka.group-id}", containerFactory = "kafkaListenerContainerFactory")
    public void processChangeEvent(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        String topicPartition = record.topic() + "-" + record.partition();
        if (offsetTrackingService.isAlreadyProcessed(topicPartition, record.offset())) {
            acknowledgment.acknowledge();
            return; // Skip already processed messages
        }

        ChangeEvent changeEvent;
        try {
            // Assuming Kafka message value is a JSON string representing ChangeEvent
            changeEvent = objectMapper.readValue(record.value(), ChangeEvent.class);
        } catch (JsonProcessingException e) {
            System.err.println("Error parsing change event from Kafka: " + e.getMessage());
            acknowledgment.acknowledge(); // Acknowledge to move past bad message
            return;
        }

        System.out.println("Received change event: " + changeEvent);

        String primaryKeyIdentifier = getPrimaryKeyIdentifier(changeEvent);
        if (primaryKeyIdentifier == null) {
            System.err.println("Could not determine primary key for event: " + changeEvent);
            acknowledgment.acknowledge();
            return;
        }

        // Conflict detection and resolution logic
        ChangeEvent existingEvent = latestEvents.get(primaryKeyIdentifier);
        if (existingEvent != null) {
            // Potential conflict detected
            ConflictRecord conflictRecord = conflictResolverService.resolveConflict(existingEvent, changeEvent);
            if (conflictRecord != null) {
                System.out.println("Conflict detected and resolved: " + conflictRecord.getDescription());
                // Publish the winning event to the resolved topic
                ChangeEvent winningEvent = (conflictRecord.getWinningBranch().equals(existingEvent.getBranchId())) ? existingEvent : changeEvent;
                publishResolvedEvent(winningEvent);
                // Update latestEvents with the winning event
                latestEvents.put(primaryKeyIdentifier, winningEvent);
            } else {
                // No clear winner or same branch update, just update with the latest
                if (changeEvent.getScn() != null && existingEvent.getScn() != null && changeEvent.getScn() > existingEvent.getScn()) {
                    latestEvents.put(primaryKeyIdentifier, changeEvent);
                    publishResolvedEvent(changeEvent);
                } else if (changeEvent.getScn() == null || existingEvent.getScn() == null) {
                    // If SCNs are not available, use timestamp or just update with current event
                    if (changeEvent.getCommitTimestamp() != null && existingEvent.getCommitTimestamp() != null && changeEvent.getCommitTimestamp().isAfter(existingEvent.getCommitTimestamp())) {
                        latestEvents.put(primaryKeyIdentifier, changeEvent);
                        publishResolvedEvent(changeEvent);
                    } else if (changeEvent.getCommitTimestamp() == null || existingEvent.getCommitTimestamp() == null) {
                        // Fallback: if no SCN or timestamp, just take the latest received
                        latestEvents.put(primaryKeyIdentifier, changeEvent);
                        publishResolvedEvent(changeEvent);
                    }
                }
            }
        } else {
            // No existing event, just add it and publish as resolved
            latestEvents.put(primaryKeyIdentifier, changeEvent);
            publishResolvedEvent(changeEvent);
        }

        offsetTrackingService.recordProcessedOffset(topicPartition, record.offset());
        acknowledgment.acknowledge();
    }

    private String getPrimaryKeyIdentifier(ChangeEvent event) {
        if (event.getPrimaryKey() == null || event.getPrimaryKey().isEmpty()) {
            return null;
        }
        // Create a unique identifier for the record based on schema, table, and primary key
        return String.format("%s.%s.%s", event.getSchemaName(), event.getTableName(), event.getPrimaryKey().toString());
    }

    private void publishResolvedEvent(ChangeEvent event) {
        String resolvedTopic = resolvedTopicPrefix + event.getSchemaName().toLowerCase();
        kafkaTemplate.send(resolvedTopic, event.getPrimaryKey().toString(), event);
        System.out.println("Published resolved event to topic: " + resolvedTopic + " for record: " + event.getPrimaryKey());
    }
}
