package com.hadoop.migration.model;

public enum MigrationStatus {
    PENDING,
    METADATA_EXTRACTING,
    DATA_COPYING,
    DATA_COPIED,
    METADATA_MIGRATING,
    COMPLETED,
    FAILED,
    ROLLBACK,
    ROLLBACK_COMPLETE
}
