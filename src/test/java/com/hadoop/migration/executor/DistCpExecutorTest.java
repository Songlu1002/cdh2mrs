package com.hadoop.migration.executor;

import com.hadoop.migration.config.DistcpConfig;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DistCpExecutorTest {

    @Test
    void testBuildCommand() {
        DistcpConfig distcpConfig = new DistcpConfig();
        distcpConfig.setMapTasks(20);
        distcpConfig.setBandwidthMB(100);

        DistCpExecutor executor = new DistCpExecutor(distcpConfig, "/opt/hadoop");

        List<String> cmd = executor.buildCommand(
            "webhdfs://cdh-nn:9870/source/db/table",
            "webhdfs://mrs-nn:9870/target/db/table");

        assertEquals("/opt/hadoop/bin/hadoop", cmd.get(0));
        assertEquals("distcp", cmd.get(1));
        assertTrue(cmd.contains("-skipcrccheck"));
        assertTrue(cmd.contains("-update"));
        assertTrue(cmd.contains("-m"));
        assertTrue(cmd.contains("-bandwidth"));
        assertTrue(cmd.contains("-webhdfs"));
        assertEquals("webhdfs://cdh-nn:9870/source/db/table", cmd.get(cmd.size() - 2));
        assertEquals("webhdfs://mrs-nn:9870/target/db/table", cmd.get(cmd.size() - 1));
    }

    @Test
    void testExecutionResult() {
        DistCpExecutor.ExecutionResult success =
            DistCpExecutor.ExecutionResult.success("Copy complete");
        assertTrue(success.isSuccess());
        assertEquals(0, success.getExitCode());

        DistCpExecutor.ExecutionResult failure =
            DistCpExecutor.ExecutionResult.failure(1, "Error");
        assertFalse(failure.isSuccess());
        assertEquals(1, failure.getExitCode());
    }
}