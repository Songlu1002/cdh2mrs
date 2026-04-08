package com.hadoop.migration.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MigrationState {
    private String sourceCluster;
    private String targetCluster;
    private long startTime;
    private long endTime;
    private Map<String, MigrationResult> tableResults;  // key: "database.table"

    public MigrationState() {
        this.tableResults = new HashMap<>();
        this.startTime = System.currentTimeMillis();
    }

    public String tableKey(String database, String table) {
        return database + "." + table;
    }

    public void putResult(String database, String table, MigrationResult result) {
        tableResults.put(tableKey(database, table), result);
    }

    public MigrationResult getResult(String database, String table) {
        return tableResults.get(tableKey(database, table));
    }

    public List<MigrationResult> getAllResults() {
        return new ArrayList<>(tableResults.values());
    }

    public int getTotalCount() { return tableResults.size(); }

    public int getCompletedCount() {
        return (int) tableResults.values().stream()
            .filter(r -> r.getStatus() == MigrationStatus.COMPLETED)
            .count();
    }

    public int getFailedCount() {
        return (int) tableResults.values().stream()
            .filter(r -> r.getStatus() == MigrationStatus.FAILED)
            .count();
    }

    // Getters and setters
    public String getSourceCluster() { return sourceCluster; }
    public void setSourceCluster(String sourceCluster) { this.sourceCluster = sourceCluster; }

    public String getTargetCluster() { return targetCluster; }
    public void setTargetCluster(String targetCluster) { this.targetCluster = targetCluster; }

    public long getStartTime() { return startTime; }
    public void setStartTime(long startTime) { this.startTime = startTime; }

    public long getEndTime() { return endTime; }
    public void setEndTime(long endTime) { this.endTime = endTime; }

    public Map<String, MigrationResult> getTableResults() { return tableResults; }
    public void setTableResults(Map<String, MigrationResult> tableResults) { this.tableResults = tableResults; }
}
