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
        public MigrationResult build() {
            result.endTime = Instant.now().toEpochMilli();
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

    public long getDurationMs() {
        if (endTime < 0) {
            return -1; // Migration not completed yet
        }
        return endTime - startTime;
    }

    public boolean isSuccess() { return status == MigrationStatus.COMPLETED; }
}
