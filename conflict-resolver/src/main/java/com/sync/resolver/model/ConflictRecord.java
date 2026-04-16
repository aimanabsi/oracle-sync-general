package com.sync.resolver.model;

import java.time.LocalDateTime;

public class ConflictRecord {
    private String conflictId;
    private String schemaName;
    private String tableName;
    private String primaryKey;
    private LocalDateTime conflictTimestamp;
    private String winningBranch;
    private String losingBranch;
    private String resolutionStrategy;
    private String description;

    // Getters and Setters
    public String getConflictId() {
        return conflictId;
    }

    public void setConflictId(String conflictId) {
        this.conflictId = conflictId;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public LocalDateTime getConflictTimestamp() {
        return conflictTimestamp;
    }

    public void setConflictTimestamp(LocalDateTime conflictTimestamp) {
        this.conflictTimestamp = conflictTimestamp;
    }

    public String getWinningBranch() {
        return winningBranch;
    }

    public void setWinningBranch(String winningBranch) {
        this.winningBranch = winningBranch;
    }

    public String getLosingBranch() {
        return losingBranch;
    }

    public void setLosingBranch(String losingBranch) {
        this.losingBranch = losingBranch;
    }

    public String getResolutionStrategy() {
        return resolutionStrategy;
    }

    public void setResolutionStrategy(String resolutionStrategy) {
        this.resolutionStrategy = resolutionStrategy;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
