package com.hadoop.migration.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ExecutionConfigTest {

    @Test
    void testDefaultValues() {
        ExecutionConfig config = new ExecutionConfig();
        assertTrue(config.isContinueOnFailure());
        assertEquals(0, config.getMaxFailedTasks());
        assertEquals(1, config.getBatchConcurrency());
    }

    @Test
    void testSetters() {
        ExecutionConfig config = new ExecutionConfig();
        config.setContinueOnFailure(false);
        config.setMaxFailedTasks(10);
        config.setBatchConcurrency(5);

        assertFalse(config.isContinueOnFailure());
        assertEquals(10, config.getMaxFailedTasks());
        assertEquals(5, config.getBatchConcurrency());
    }

    @Test
    void testContinueOnFailureToggle() {
        ExecutionConfig config = new ExecutionConfig();

        config.setContinueOnFailure(true);
        assertTrue(config.isContinueOnFailure());

        config.setContinueOnFailure(false);
        assertFalse(config.isContinueOnFailure());
    }

    @Test
    void testMaxFailedTasksBoundaries() {
        ExecutionConfig config = new ExecutionConfig();

        config.setMaxFailedTasks(0);
        assertEquals(0, config.getMaxFailedTasks());

        config.setMaxFailedTasks(100);
        assertEquals(100, config.getMaxFailedTasks());
    }
}
