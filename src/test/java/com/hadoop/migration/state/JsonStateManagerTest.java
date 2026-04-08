package com.hadoop.migration.state;

import com.hadoop.migration.model.MigrationResult;
import com.hadoop.migration.model.MigrationState;
import com.hadoop.migration.model.MigrationStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class JsonStateManagerTest {

    @TempDir
    Path tempDir;

    @Test
    void testInitializeAndSave() {
        JsonStateManager manager = new JsonStateManager(
            tempDir.resolve("state.json").toString());

        manager.initialize("cdh-cluster", "mrs-cluster");
        MigrationState state = manager.getState();

        assertEquals("cdh-cluster", state.getSourceCluster());
        assertEquals("mrs-cluster", state.getTargetCluster());
        assertNotNull(state.getStartTime());
    }

    @Test
    void testRecordResult() {
        String stateFile = tempDir.resolve("state.json").toString();
        JsonStateManager manager = new JsonStateManager(stateFile);

        manager.initialize("cdh", "mrs");

        MigrationResult result = MigrationResult.builder()
            .database("db1")
            .table("table1")
            .status(MigrationStatus.COMPLETED)
            .dataSize(1024L)
            .build();
        manager.recordResult(result);

        MigrationState loaded = manager.loadState();
        assertEquals(1, loaded.getTotalCount());
        assertEquals(1, loaded.getCompletedCount());
    }

    @Test
    void testLoadNonExistent() {
        JsonStateManager manager = new JsonStateManager(
            tempDir.resolve("nonexistent.json").toString());

        assertFalse(manager.hasState());
        assertNull(manager.loadState());
    }

    @Test
    void testMarkCompleted() {
        String stateFile = tempDir.resolve("state.json").toString();
        JsonStateManager manager = new JsonStateManager(stateFile);

        manager.initialize("cdh", "mrs");
        manager.markCompleted();

        MigrationState loaded = manager.loadState();
        assertNotNull(loaded.getEndTime());
    }
}