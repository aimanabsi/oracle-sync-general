package com.sync.monitor.model;

import java.time.LocalDateTime;

public class BranchStatus {
    private String branchId;
    private String name;
    private String status; // e.g., Online, Offline, Degraded
    private LocalDateTime lastSeen;
    private String ipAddress;
    private Long syncLagSeconds; // How far behind is this branch from hub

    public BranchStatus() {
    }

    public BranchStatus(String branchId, String name, String status, LocalDateTime lastSeen, String ipAddress, Long syncLagSeconds) {
        this.branchId = branchId;
        this.name = name;
        this.status = status;
        this.lastSeen = lastSeen;
        this.ipAddress = ipAddress;
        this.syncLagSeconds = syncLagSeconds;
    }

    // Getters and Setters
    public String getBranchId() {
        return branchId;
    }

    public void setBranchId(String branchId) {
        this.branchId = branchId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public LocalDateTime getLastSeen() {
        return lastSeen;
    }

    public void setLastSeen(LocalDateTime lastSeen) {
        this.lastSeen = lastSeen;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public Long getSyncLagSeconds() {
        return syncLagSeconds;
    }

    public void setSyncLagSeconds(Long syncLagSeconds) {
        this.syncLagSeconds = syncLagSeconds;
    }
}
