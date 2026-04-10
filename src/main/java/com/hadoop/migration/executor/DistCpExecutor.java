package com.hadoop.migration.executor;

import com.hadoop.migration.config.DistcpConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DistCpExecutor {
    private static final Logger log = LoggerFactory.getLogger(DistCpExecutor.class);

    private final DistcpConfig config;
    private final String hadoopHome;

    public DistCpExecutor(DistcpConfig config, String hadoopHome) {
        this.config = config;
        this.hadoopHome = hadoopHome;
    }

    public DistCpExecutor(DistcpConfig config) {
        this(config, System.getenv("HADOOP_HOME"));
    }

    /**
     * Closes the executor. Since DistCp executes as an external process,
     * this method is a no-op provided for consistency with other resources.
     */
    public void close() {
        // No-op: DistCp process completes after execute()
    }

    public ExecutionResult execute(String sourcePath, String targetPath) {
        log.info("Starting DistCp: {} -> {}", sourcePath, targetPath);

        List<String> command = buildCommand(sourcePath, targetPath);
        log.debug("Command: {}", String.join(" ", command));

        try {
            ProcessBuilder pb = new ProcessBuilder(command)
                .redirectErrorStream(true);
            Process process = pb.start();

            StringBuilder output = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append("\n");
                    log.debug("DistCp: {}", line);
                }
            }

            boolean finished = process.waitFor(config.getTimeoutMinutes(), TimeUnit.MINUTES);
            int exitCode = finished ? process.exitValue() : -1;

            if (exitCode == 0) {
                log.info("DistCp completed successfully");
                return ExecutionResult.success(output.toString());
            } else {
                log.error("DistCp failed with exit code {}", exitCode);
                return ExecutionResult.failure(exitCode, output.toString());
            }
        } catch (IOException e) {
            log.error("Failed to execute DistCp", e);
            return ExecutionResult.failure(-1, e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ExecutionResult.failure(-1, "Interrupted");
        }
    }

    /**
     * Builds the platform-appropriate Hadoop command path.
     * Handles Windows vs Unix path differences and validates the hadoop binary exists.
     */
    private String buildHadoopCommand() {
        if (hadoopHome == null || hadoopHome.isEmpty()) {
            throw new IllegalStateException(
                "HADOOP_HOME environment variable is not set. " +
                "Please set HADOOP_HOME to the Hadoop installation directory.");
        }

        // Validate hadoopHome path - prevent command injection
        Path hadoopPath = Path.of(hadoopHome).normalize();
        if (!hadoopPath.toFile().exists()) {
            throw new IllegalStateException(
                "HADOOP_HOME does not exist: " + hadoopHome);
        }

        // Use webhdfs protocol - detect OS and use appropriate script
        String os = System.getProperty("os.name", "").toLowerCase();
        if (os.contains("win")) {
            // Windows: use hadoop.cmd
            String hadoopCmd = hadoopPath.resolve("bin").resolve("hadoop.cmd").toString();
            File hadoopBinary = new File(hadoopCmd);
            if (!hadoopBinary.exists()) {
                throw new IllegalStateException(
                    "Hadoop command not found: " + hadoopCmd + ". " +
                    "Ensure Hadoop is properly installed on Windows (requires WSL or Windows-native Hadoop).");
            }
            return hadoopCmd;
        } else {
            // Unix/Linux: use hadoop shell script
            String hadoopScript = hadoopPath.resolve("bin").resolve("hadoop").toString();
            File hadoopBinary = new File(hadoopScript);
            if (!hadoopBinary.exists()) {
                throw new IllegalStateException(
                    "Hadoop command not found: " + hadoopScript);
            }
            return hadoopScript;
        }
    }

    public List<String> buildCommand(String sourcePath, String targetPath) {
        List<String> cmd = new ArrayList<>();
        cmd.add(buildHadoopCommand());
        cmd.add("distcp");
        cmd.add("-skipcrccheck");
        cmd.add("-p");
        cmd.add("-update");
        cmd.add("-strategy");
        cmd.add("dynamic");
        cmd.add("-m");
        cmd.add(String.valueOf(config.getMapTasks()));
        cmd.add("-bandwidth");
        cmd.add(String.valueOf(config.getBandwidthMB()));
        cmd.add("-webhdfs");
        cmd.add(sourcePath);
        cmd.add(targetPath);
        return cmd;
    }

    public static class ExecutionResult {
        private final boolean success;
        private final int exitCode;
        private final String output;
        private final List<String> copiedFiles;

        private ExecutionResult(boolean success, int exitCode, String output) {
            this.success = success;
            this.exitCode = exitCode;
            this.output = output;
            this.copiedFiles = new ArrayList<>();
        }

        public static ExecutionResult success(String output) {
            return new ExecutionResult(true, 0, output);
        }

        public static ExecutionResult failure(int exitCode, String output) {
            return new ExecutionResult(false, exitCode, output);
        }

        public boolean isSuccess() { return success; }
        public int getExitCode() { return exitCode; }
        public String getOutput() { return output; }
        public List<String> getCopiedFiles() { return copiedFiles; }
    }
}