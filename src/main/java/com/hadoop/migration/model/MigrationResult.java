package com.hadoop.migration.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

public class MigrationResult {
    private String database;
    private String table;
    private MigrationStatus status;
    private String errorMessage;
    private String errorCode;
    private long startTime;
    private long endTime;

    @JsonProperty("dataSizeBytes")
    private long dataSizeBytes;

    public MigrationResult() {
        this.startTime = Instant.now().toEpochMilli();
        this.endTime = -1; // -1 indicates not completed yet
    }

    // Builder pattern for convenience
    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private final MigrationResult result = new MigrationResult();

        public Builder database(String db) { result.database = db; return this; }
        public Builder table(String table) { result.table = table; return this; }
        public Builder status(MigrationStatus status) { result.status = status; return this; }
        public Builder error(String errorCode, String message) {
            result.errorCode = errorCode;
            result.errorMessage = message;
            return this;
        }
        public Builder dataSize(long bytes) { result.dataSizeBytes = bytes; return this; }
        public Builder startTime(long startTime) { result.startTime = startTime; return this; }
        public Builder endTime(long endTime) { result.endTime = endTime; return this; }

        public MigrationResult build() {
            // Only set endTime if status is terminal (COMPLETED or FAILED)
            if (result.status == MigrationStatus.COMPLETED || result.status == MigrationStatus.FAILED) {
                if (result.endTime < 0) {
                    result.endTime = Instant.now().toEpochMilli();
                }
            }
            return result;
        }
    }

    // Getters
    public String getDatabase() { return database; }
    public String getTable() { return table; }
    public MigrationStatus getStatus() { return status; }
    public String getErrorMessage() { return errorMessage; }
    public String getErrorCode() { return errorCode; }
    public long getStartTime() { return startTime; }
    public long getEndTime() { return endTime; }
    public long getDataSizeBytes() { return dataSizeBytes; }

    // Setters for updating fields during migration
    public void setDatabase(String database) { this.database = database; }
    public void setTable(String table) { this.table = table; }
    public void setStatus(MigrationStatus status) { this.status = status; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    public void setErrorCode(String errorCode) { this.errorCode = errorCode; }
    public void setStartTime(long startTime) { this.startTime = startTime; }
    public void setEndTime(long endTime) { this.endTime = endTime; }
    public void setDataSizeBytes(long dataSizeBytes) { this.dataSizeBytes = dataSizeBytes; }

    public long getDurationMs() {
        if (endTime < 0) {
            return -1; // Migration not completed yet
        }
        return endTime - startTime;
    }

    public boolean isSuccess() { return status == MigrationStatus.COMPLETED; }

    /**
     * Creates a copy of this result with updated status, preserving other fields.
     */
    public MigrationResult withStatus(MigrationStatus newStatus) {
        MigrationResult copy = new MigrationResult();
        copy.database = this.database;
        copy.table = this.table;
        copy.status = newStatus;
        copy.errorMessage = this.errorMessage;
        copy.errorCode = this.errorCode;
        copy.startTime = this.startTime;
        copy.dataSizeBytes = this.dataSizeBytes;

        // Set endTime only for terminal statuses
        if (newStatus == MigrationStatus.COMPLETED || newStatus == MigrationStatus.FAILED) {
            copy.endTime = Instant.now().toEpochMilli();
        } else {
            copy.endTime = -1;
        }
        return copy;
    }
}
