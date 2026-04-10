package com.hadoop.migration.config;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AppConfigTest {

    @Test
    void testDefaultValues() {
        AppConfig config = new AppConfig();
        assertNull(config.getClusters());
        assertNull(config.getMigration());
    }

    @Test
    void testClusters() {
        AppConfig config = new AppConfig();

        ClusterConfig source = new ClusterConfig();
        source.setName("source-cluster");

        ClusterConfig target = new ClusterConfig();
        target.setName("target-cluster");

        AppConfig.Clusters clusters = new AppConfig.Clusters();
        clusters.setSource(source);
        clusters.setTarget(target);

        config.setClusters(clusters);

        assertNotNull(config.getClusters());
        assertEquals("source-cluster", config.getClusters().getSource().getName());
        assertEquals("target-cluster", config.getClusters().getTarget().getName());
    }

    @Test
    void testMigrationConfig() {
        AppConfig config = new AppConfig();

        AppConfig.MigrationConfig migrationConfig = new AppConfig.MigrationConfig();
        DistcpConfig distcp = new DistcpConfig();
        distcp.setMapTasks(20);
        migrationConfig.setDistcp(distcp);

        ExecutionConfig exec = new ExecutionConfig();
        exec.setContinueOnFailure(true);
        migrationConfig.setExecution(exec);

        AppConfig.OutputConfig output = new AppConfig.OutputConfig();
        output.setStateFile("test-state.json");
        migrationConfig.setOutput(output);

        config.setMigration(migrationConfig);

        assertNotNull(config.getMigration());
        assertEquals(20, config.getMigration().getDistcp().getMapTasks());
        assertTrue(config.getMigration().getExecution().isContinueOnFailure());
        assertEquals("test-state.json", config.getMigration().getOutput().getStateFile());
    }

    @Test
    void testOutputConfigDefaults() {
        AppConfig.OutputConfig output = new AppConfig.OutputConfig();
        assertEquals("state/migration-state.json", output.getStateFile());
        assertEquals("reports/", output.getReportDir());
    }

    @Test
    void testMigrationTaskList() {
        AppConfig.MigrationConfig config = new AppConfig.MigrationConfig();
        MigrationTask task = new MigrationTask();
        task.setDatabase("test_db");
        task.setTables(List.of("table1", "table2"));

        config.setTasks(List.of(task));

        assertEquals(1, config.getTasks().size());
        assertEquals("test_db", config.getTasks().get(0).getDatabase());
        assertEquals(2, config.getTasks().get(0).getTables().size());
    }

    @Test
    void testIsMigrateAllTables() {
        MigrationTask task = new MigrationTask();

        // Not "all" - should return false
        task.setTables(List.of("table1"));
        assertFalse(task.isMigrateAllTables());

        // "all" case-insensitive - should return true
        task.setTables(List.of("ALL"));
        assertTrue(task.isMigrateAllTables());

        task.setTables(List.of("all"));
        assertTrue(task.isMigrateAllTables());

        task.setTables(List.of("All"));
        assertTrue(task.isMigrateAllTables());

        // Multiple items including "all" - should return false
        task.setTables(List.of("all", "table2"));
        assertFalse(task.isMigrateAllTables());

        // Empty list - should return false
        task.setTables(Collections.emptyList());
        assertFalse(task.isMigrateAllTables());

        // Null - should return false
        task.setTables(null);
        assertFalse(task.isMigrateAllTables());
    }
}
