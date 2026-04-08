package com.hadoop.migration.state;

import com.hadoop.migration.model.MigrationResult;
import com.hadoop.migration.model.MigrationState;
import com.hadoop.migration.model.MigrationStatus;

public interface StateManager {
    void initialize(String sourceCluster, String targetCluster);
    void saveState(MigrationState state);
    MigrationState loadState();
    void updateTableStatus(String database, String table, MigrationStatus status);
    void recordResult(MigrationResult result);
    void markCompleted();
    boolean hasState();
}