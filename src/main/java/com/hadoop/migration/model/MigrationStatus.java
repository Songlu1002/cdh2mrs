package com.hadoop.migration.model;

public enum MigrationStatus {
    PENDING,
    DATA_COPYING,
    DATA_COPIED,
    METADATA_MIGRATING,
    COMPLETED,
    FAILED,
    ROLLBACK,
    ROLLBACK_COMPLETE
}
