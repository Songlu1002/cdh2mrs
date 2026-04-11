package com.hadoop.migration.executor;

import com.hadoop.migration.config.DistcpConfig;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DistCpExecutorTest {

    private DistCpExecutor createExecutor(Path tempHadoopHome) {
        DistcpConfig distcpConfig = new DistcpConfig();
        distcpConfig.setMapTasks(20);
        distcpConfig.setBandwidthMB(100);
        distcpConfig.setSourceProtocol("webhdfs");
        return new DistCpExecutor(distcpConfig, tempHadoopHome.toString());
    }

    @Test
    void testBuildCommand() throws IOException {
        Path tempHadoopHome = Files.createTempDirectory("hadoop-test");
        Files.createDirectories(tempHadoopHome.resolve("bin"));
        Files.createFile(tempHadoopHome.resolve("bin").resolve("hadoop"));
        Files.createFile(tempHadoopHome.resolve("bin").resolve("hadoop.cmd"));

        DistCpExecutor executor = createExecutor(tempHadoopHome);

        List<String> cmd = executor.buildCommand(
            "webhdfs://cdh-nn:9870/source/db/table",
            "webhdfs://mrs-nn:9870/target/db/table");

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
    void testBuildCommandWithHftpProtocol() throws IOException {
        Path tempHadoopHome = Files.createTempDirectory("hadoop-test");
        Files.createDirectories(tempHadoopHome.resolve("bin"));
        Files.createFile(tempHadoopHome.resolve("bin").resolve("hadoop"));
        Files.createFile(tempHadoopHome.resolve("bin").resolve("hadoop.cmd"));

        DistcpConfig distcpConfig = new DistcpConfig();
        distcpConfig.setSourceProtocol("hftp");
        DistCpExecutor executor = new DistCpExecutor(distcpConfig, tempHadoopHome.toString());

        List<String> cmd = executor.buildCommand(
            "hftp://cdh-nn:9870/source/db/table",
            "webhdfs://mrs-nn:9870/target/db/table");

        assertTrue(cmd.contains("-hftp"), "Should contain -hftp protocol flag");
    }

    @Test
    void testBuildCommandWithCustomParams() throws IOException {
        Path tempHadoopHome = Files.createTempDirectory("hadoop-test");
        Files.createDirectories(tempHadoopHome.resolve("bin"));
        Files.createFile(tempHadoopHome.resolve("bin").resolve("hadoop"));
        Files.createFile(tempHadoopHome.resolve("bin").resolve("hadoop.cmd"));

        DistcpConfig distcpConfig = new DistcpConfig();
        distcpConfig.setMapTasks(50);
        distcpConfig.setBandwidthMB(200);
        distcpConfig.setSourceProtocol("webhdfs");
        DistCpExecutor executor = new DistCpExecutor(distcpConfig, tempHadoopHome.toString());

        List<String> cmd = executor.buildCommand(
            "webhdfs://cdh-nn:9870/source",
            "webhdfs://mrs-nn:9870/target");

        assertTrue(cmd.contains("-m"));
        int mIndex = cmd.indexOf("-m");
        assertEquals("50", cmd.get(mIndex + 1));

        assertTrue(cmd.contains("-bandwidth"));
        int bwIndex = cmd.indexOf("-bandwidth");
        assertEquals("200", cmd.get(bwIndex + 1));
    }

    @Test
    void testCloseIsNoOp() throws IOException {
        Path tempHadoopHome = Files.createTempDirectory("hadoop-test");
        Files.createDirectories(tempHadoopHome.resolve("bin"));
        Files.createFile(tempHadoopHome.resolve("bin").resolve("hadoop"));

        DistCpExecutor executor = createExecutor(tempHadoopHome);
        // Should not throw
        executor.close();
    }

    @Test
    void testCleanupTargetWithNullPath() throws IOException {
        Path tempHadoopHome = Files.createTempDirectory("hadoop-test");
        Files.createDirectories(tempHadoopHome.resolve("bin"));
        Files.createFile(tempHadoopHome.resolve("bin").resolve("hadoop"));

        DistCpExecutor executor = createExecutor(tempHadoopHome);
        assertFalse(executor.cleanupTarget(null));
    }

    @Test
    void testCleanupTargetWithEmptyPath() throws IOException {
        Path tempHadoopHome = Files.createTempDirectory("hadoop-test");
        Files.createDirectories(tempHadoopHome.resolve("bin"));
        Files.createFile(tempHadoopHome.resolve("bin").resolve("hadoop"));

        DistCpExecutor executor = createExecutor(tempHadoopHome);
        assertFalse(executor.cleanupTarget(""));
    }

    @Test
    void testExecutionResult() {
        DistCpExecutor.ExecutionResult success =
            DistCpExecutor.ExecutionResult.success("Copy complete");
        assertTrue(success.isSuccess());
        assertEquals(0, success.getExitCode());
        assertEquals("Copy complete", success.getOutput());
        assertTrue(success.getCopiedFiles().isEmpty());

        DistCpExecutor.ExecutionResult failure =
            DistCpExecutor.ExecutionResult.failure(1, "Error");
        assertFalse(failure.isSuccess());
        assertEquals(1, failure.getExitCode());
        assertEquals("Error", failure.getOutput());
    }

    @Test
    void testBuildCommandValidation() throws IOException {
        Path tempHadoopHome = Files.createTempDirectory("hadoop-test");

        DistcpConfig distcpConfig = new DistcpConfig();
        distcpConfig.setSourceProtocol("webhdfs");

        // Should throw because hadoop binary doesn't exist
        assertThrows(IllegalStateException.class, () -> {
            new DistCpExecutor(distcpConfig, tempHadoopHome.toString())
                .buildCommand("source", "target");
        });
    }

    @Test
    void testBuildCommandWithNullHadoopHome() {
        DistcpConfig distcpConfig = new DistcpConfig();
        distcpConfig.setSourceProtocol("webhdfs");

        assertThrows(IllegalStateException.class, () -> {
            new DistCpExecutor(distcpConfig, (String) null)
                .buildCommand("source", "target");
        });
    }

    @Test
    void testBuildCommandWithEmptyHadoopHome() {
        DistcpConfig distcpConfig = new DistcpConfig();
        distcpConfig.setSourceProtocol("webhdfs");

        assertThrows(IllegalStateException.class, () -> {
            new DistCpExecutor(distcpConfig, "")
                .buildCommand("source", "target");
        });
    }
}