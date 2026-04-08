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
}
