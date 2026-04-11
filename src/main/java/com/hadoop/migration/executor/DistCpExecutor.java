package com.hadoop.migration.executor;

import com.hadoop.migration.config.DistcpConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
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

        int maxRetries = config.getRetryCount();
        ExecutionResult lastResult = null;

        for (int attempt = 1; attempt <= maxRetries + 1; attempt++) {
            if (attempt > 1) {
                // Exponential backoff: 10s, 20s, 40s, ...
                long backoffMs = 10_000L * (1L << (attempt - 2));
                log.info("Retry attempt {}/{} after {}ms backoff", attempt - 1, maxRetries, backoffMs);
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return ExecutionResult.failure(-1, "Interrupted during retry backoff");
                }
            }

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
                int exitCode;
                if (!finished) {
                    // Timeout - forcibly terminate to prevent zombie process
                    process.destroyForcibly();
                    log.error("DistCp timed out after {} minutes on attempt {}", config.getTimeoutMinutes(), attempt);
                    exitCode = -1;
                } else {
                    exitCode = process.exitValue();
                }

                if (exitCode == 0) {
                    log.info("DistCp completed successfully");
                    return ExecutionResult.success(output.toString());
                } else {
                    log.warn("DistCp attempt {} failed with exit code {}", attempt, exitCode);
                    lastResult = ExecutionResult.failure(exitCode, output.toString());
                }
            } catch (IOException e) {
                log.error("Failed to execute DistCp on attempt {}", attempt, e);
                lastResult = ExecutionResult.failure(-1, e.getMessage());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return ExecutionResult.failure(-1, "Interrupted");
            }
        }

        log.error("DistCp failed after {} attempts", maxRetries + 1);
        return lastResult;
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

        // Use configured protocol - DistCp only needs source protocol flag
        String sourceProtocol = config.getSourceProtocol();

        cmd.add("-skipcrccheck");
        cmd.add("-p");
        cmd.add("-update");
        cmd.add("-strategy");
        cmd.add("dynamic");
        cmd.add("-m");
        cmd.add(String.valueOf(config.getMapTasks()));
        cmd.add("-bandwidth");
        cmd.add(String.valueOf(config.getBandwidthMB()));

        // Always add source protocol flag to ensure correct protocol is used
        cmd.add("-" + sourceProtocol.toLowerCase());

        cmd.add(sourcePath);
        cmd.add(targetPath);
        return cmd;
    }

    /**
     * Deletes the target path on HDFS using WebHDFS.
     * Used for cleanup when migration fails after data copy.
     * @param targetPath the HDFS path to delete
     * @return true if deletion succeeded, false otherwise
     */
    public boolean cleanupTarget(String targetPath) {
        if (targetPath == null || targetPath.isEmpty()) {
            log.warn("No target path provided for cleanup, skipping");
            return false;
        }

        log.info("Cleaning up orphaned data at target: {}", targetPath);

        try {
            String hadoopCmd = buildHadoopCommand();
            // Extract the actual path using proper URI parsing to prevent path traversal
            String pathToDelete;
            try {
                URI uri = new URI(targetPath);
                pathToDelete = uri.getPath();
                if (pathToDelete == null || pathToDelete.isEmpty()) {
                    log.warn("Could not extract valid path from URI: {}, using as-is", targetPath);
                    pathToDelete = targetPath;
                }
            } catch (Exception e) {
                // Fallback: if URI parsing fails, use the path as-is
                // Only use the part after protocol if it looks like a path
                if (targetPath.contains("://")) {
                    String[] parts = targetPath.split("://", 2);
                    if (parts.length > 1 && parts[1].contains("/")) {
                        pathToDelete = "/" + parts[1].substring(parts[1].indexOf("/") + 1);
                    } else {
                        pathToDelete = targetPath;
                    }
                } else {
                    pathToDelete = targetPath;
                }
            }

            // Validate path doesn't contain suspicious patterns
            if (pathToDelete.contains("..") || pathToDelete.contains("~")) {
                log.error("Suspicious path pattern detected in cleanup target: {}", pathToDelete);
                return false;
            }

            List<String> command = List.of(
                hadoopCmd, "dfs", "-rm", "-r", "-skipTrash", pathToDelete
            );

            log.debug("Cleanup command: {}", String.join(" ", command));

            ProcessBuilder pb = new ProcessBuilder(command)
                .redirectErrorStream(true);
            Process process = pb.start();

            StringBuilder output = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append("\n");
                    log.debug("Cleanup: {}", line);
                }
            }

            boolean finished = process.waitFor(5, TimeUnit.MINUTES);
            if (!finished) {
                // Process timed out - forcibly terminate to prevent zombie process
                process.destroyForcibly();
                log.error("Cleanup timed out after 5 minutes for path: {}", pathToDelete);
                return false;
            }
            int exitCode = process.exitValue();

            if (exitCode == 0) {
                log.info("Cleanup completed successfully for: {}", targetPath);
                return true;
            } else {
                log.warn("Cleanup failed for {} with exit code {}: {}",
                    targetPath, exitCode, output.toString());
                return false;
            }
        } catch (Exception e) {
            log.error("Failed to cleanup target path: {}", targetPath, e);
            return false;
        }
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