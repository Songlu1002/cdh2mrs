package com.hadoop.migration.executor;

import com.hadoop.migration.config.DistcpConfig;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DistCpExecutorTest {

    @Test
    void testBuildCommand() throws IOException {
        // Create temp Hadoop home for testing
        Path tempHadoopHome = Files.createTempDirectory("hadoop-test");
        Path tempBin = tempHadoopHome.resolve("bin");
        Files.createDirectories(tempBin);

        // Create dummy hadoop script (hadoop on unix, hadoop.cmd on windows)
        Files.createFile(tempHadoopHome.resolve("bin").resolve("hadoop"));
        Files.createFile(tempHadoopHome.resolve("bin").resolve("hadoop.cmd"));

        DistcpConfig distcpConfig = new DistcpConfig();
        distcpConfig.setMapTasks(20);
        distcpConfig.setBandwidthMB(100);

        DistCpExecutor executor = new DistCpExecutor(distcpConfig, tempHadoopHome.toString());

        List<String> cmd = executor.buildCommand(
            "webhdfs://cdh-nn:9870/source/db/table",
            "webhdfs://mrs-nn:9870/target/db/table");

        // Should use hadoop.cmd on Windows, hadoop on Unix
        String os = System.getProperty("os.name", "").toLowerCase();
        String expectedHadoopCmd = os.contains("win")
            ? tempHadoopHome.resolve("bin").resolve("hadoop.cmd").toString()
            : tempHadoopHome.resolve("bin").resolve("hadoop").toString();
        assertEquals(expectedHadoopCmd, cmd.get(0));
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