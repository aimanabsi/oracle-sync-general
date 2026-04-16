package com.sync.monitor.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/sync-status")
public class SyncStatusController {

    @GetMapping
    public Map<String, String> getSyncStatus() {
        // Placeholder for actual sync status logic
        return Map.of("status", "Operational", "lastSync", "2024-04-16T10:30:00Z");
    }
}
