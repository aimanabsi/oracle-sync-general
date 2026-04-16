package com.sync.resolver.model;

import java.util.Map;
import java.time.LocalDateTime;

public class ChangeEvent {
    private String schemaName;
    private String tableName;
    private String operationType; // e.g., CREATE, UPDATE, DELETE
    private Map<String, Object> before; // Old values for UPDATE/DELETE
    private Map<String, Object> after;  // New values for CREATE/UPDATE
    private Long scn; // Oracle System Change Number
    private LocalDateTime commitTimestamp; // Commit timestamp from Oracle
    private String branchId; // Identifier for the branch where the change originated
    private Map<String, Object> primaryKey; // Primary key fields and values

    // Constructors
    public ChangeEvent() {
    }

    public ChangeEvent(String schemaName, String tableName, String operationType, Map<String, Object> before, Map<String, Object> after, Long scn, LocalDateTime commitTimestamp, String branchId, Map<String, Object> primaryKey) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.operationType = operationType;
        this.before = before;
        this.after = after;
        this.scn = scn;
        this.commitTimestamp = commitTimestamp;
        this.branchId = branchId;
        this.primaryKey = primaryKey;
    }

    // Getters and Setters
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

    public String getOperationType() {
        return operationType;
    }

    public void setOperationType(String operationType) {
        this.operationType = operationType;
    }

    public Map<String, Object> getBefore() {
        return before;
    }

    public void setBefore(Map<String, Object> before) {
        this.before = before;
    }

    public Map<String, Object> getAfter() {
        return after;
    }

    public void setAfter(Map<String, Object> after) {
        this.after = after;
    }

    public Long getScn() {
        return scn;
    }

    public void setScn(Long scn) {
        this.scn = scn;
    }

    public LocalDateTime getCommitTimestamp() {
        return commitTimestamp;
    }

    public void setCommitTimestamp(LocalDateTime commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }

    public String getBranchId() {
        return branchId;
    }

    public void setBranchId(String branchId) {
        this.branchId = branchId;
    }

    public Map<String, Object> getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(Map<String, Object> primaryKey) {
        this.primaryKey = primaryKey;
    }

    @Override
    public String toString() {
        return "ChangeEvent{" +
               "schemaName=\'" + schemaName + "\'," +
               "tableName=\'" + tableName + "\'," +
               "operationType=\'" + operationType + "\'," +
               "before=" + before +
               ", after=" + after +
               ", scn=" + scn +
               ", commitTimestamp=" + commitTimestamp +
               ", branchId=\'" + branchId + "\'," +
               ", primaryKey=" + primaryKey +
               "}";
    }
}
