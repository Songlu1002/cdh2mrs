package com.hadoop.migration.config;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MigrationTaskTest {

    @Test
    void testSettersAndGetters() {
        MigrationTask task = new MigrationTask();
        task.setDatabase("sales_db");
        task.setTables(List.of("orders", "customers"));

        assertEquals("sales_db", task.getDatabase());
        assertEquals(2, task.getTables().size());
        assertEquals("orders", task.getTables().get(0));
        assertEquals("customers", task.getTables().get(1));
    }

    @Test
    void testIsMigrateAllTables() {
        MigrationTask task = new MigrationTask();

        // Null tables
        task.setTables(null);
        assertFalse(task.isMigrateAllTables());

        // Empty list
        task.setTables(List.of());
        assertFalse(task.isMigrateAllTables());

        // Single "all"
        task.setTables(List.of("all"));
        assertTrue(task.isMigrateAllTables());

        // Single "ALL" case insensitive
        task.setTables(List.of("ALL"));
        assertTrue(task.isMigrateAllTables());

        // Single "All" mixed case
        task.setTables(List.of("All"));
        assertTrue(task.isMigrateAllTables());

        // Multiple items with "all"
        task.setTables(List.of("all", "other"));
        assertFalse(task.isMigrateAllTables());

        // Regular table name
        task.setTables(List.of("orders"));
        assertFalse(task.isMigrateAllTables());
    }
}
