package com.hadoop.migration.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MigrationStateTest {

    @Test
    void testTableResultsTracking() {
        MigrationState state = new MigrationState();
        state.putResult("db1", "table1", MigrationResult.builder()
            .database("db1").table("table1").status(MigrationStatus.COMPLETED).build());
        state.putResult("db1", "table2", MigrationResult.builder()
            .database("db1").table("table2").status(MigrationStatus.FAILED)
            .error("ERR001", "Copy failed").build());

        assertEquals(2, state.getTotalCount());
        assertEquals(1, state.getCompletedCount());
        assertEquals(1, state.getFailedCount());
        assertNotNull(state.getResult("db1", "table1"));
        assertNull(state.getResult("db1", "table3"));
    }

    @Test
    void testMigrationResultDuration() {
        MigrationResult result = MigrationResult.builder()
            .database("db1").table("t1").status(MigrationStatus.COMPLETED)
            .build();

        assertTrue(result.getDurationMs() >= 0);
        assertTrue(result.isSuccess());
    }

    @Test
    void testMigrationResultWithStatus() {
        // Test withStatus preserves information when updating status
        MigrationResult original = MigrationResult.builder()
            .database("db1").table("t1")
            .status(MigrationStatus.DATA_COPYING)
            .dataSize(1024)
            .build();

        // DATA_COPYING should have endTime = -1
        assertEquals(-1, original.getEndTime());

        // Update to COMPLETED
        MigrationResult completed = original.withStatus(MigrationStatus.COMPLETED);
        assertEquals("db1", completed.getDatabase());
        assertEquals("t1", completed.getTable());
        assertEquals(MigrationStatus.COMPLETED, completed.getStatus());
        assertEquals(1024, completed.getDataSizeBytes());
        assertTrue(completed.getEndTime() >= 0); // Should have endTime now
        assertTrue(completed.isSuccess());

        // Original should be unchanged
        assertEquals(-1, original.getEndTime());
    }

    @Test
    void testMigrationResultTerminalStatus() {
        // COMPLETED should set endTime
        MigrationResult completed = MigrationResult.builder()
            .database("db").table("t").status(MigrationStatus.COMPLETED).build();
        assertTrue(completed.getEndTime() >= 0);

        // FAILED should set endTime
        MigrationResult failed = MigrationResult.builder()
            .database("db").table("t").status(MigrationStatus.FAILED)
            .error("ERR001", "Test error").build();
        assertTrue(failed.getEndTime() >= 0);

        // Non-terminal status should have endTime = -1
        MigrationResult copying = MigrationResult.builder()
            .database("db").table("t").status(MigrationStatus.DATA_COPYING).build();
        assertEquals(-1, copying.getEndTime());
    }

    @Test
    void testMigrationStatusMetadataExtracting() {
        // Test the new status
        MigrationResult result = MigrationResult.builder()
            .database("db").table("t").status(MigrationStatus.METADATA_EXTRACTING)
            .build();
        assertEquals(MigrationStatus.METADATA_EXTRACTING, result.getStatus());
        assertEquals(-1, result.getEndTime()); // Non-terminal status
    }
}
