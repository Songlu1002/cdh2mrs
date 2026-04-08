package com.hadoop.migration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.hadoop.migration.config.AppConfig;
import com.hadoop.migration.config.ClusterConfig;
import com.hadoop.migration.config.MigrationTask;
import com.hadoop.migration.executor.DistCpExecutor;
import com.hadoop.migration.model.MigrationResult;
import com.hadoop.migration.model.MigrationState;
import com.hadoop.migration.model.MigrationStatus;
import com.hadoop.migration.state.JsonStateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        if (args.length == 0 || "--help".equals(args[0]) || "-h".equals(args[0])) {
            printHelp();
            System.exit(0);
        }

        if (!"--config".equals(args[0])) {
            System.err.println("Unknown option: " + args[0]);
            System.err.println("Use --help for usage information");
            System.exit(1);
        }

        if (args.length < 2) {
            System.err.println("--config requires a file path");
            System.exit(1);
        }

        String configPath = args[1];
        runMigration(configPath);
    }

    private static void printHelp() {
        System.out.println("Hadoop Migration Tool - CDH to MRS");
        System.out.println();
        System.out.println("Usage:");
        System.out.println("  java -jar hadoop-migration-tool.jar --config <config-file>");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --config <file>   Path to configuration YAML file");
        System.out.println("  --help, -h        Show this help message");
        System.out.println();
        System.out.println("Example:");
        System.out.println("  java -jar hadoop-migration-tool.jar --config conf/config.yaml");
    }

    public static void runMigration(String configPath) {
        log.info("=== Hadoop Migration Tool Starting ===");
        log.info("Config: {}", configPath);

        try {
            // 1. Load configuration
            AppConfig config = loadConfig(configPath);
            log.info("Loaded config for {} -> {}",
                config.getClusters().getSource().getName(),
                config.getClusters().getTarget().getName());

            // 2. Initialize state manager
            String stateFile = config.getMigration().getOutput().getStateFile();
            JsonStateManager stateManager = new JsonStateManager(stateFile);
            stateManager.initialize(
                config.getClusters().getSource().getName(),
                config.getClusters().getTarget().getName()
            );

            // 3. Execute migration for each task
            boolean overallSuccess = true;
            for (MigrationTask task : config.getMigration().getTasks()) {
                log.info("Processing database: {}", task.getDatabase());

                for (String tableName : task.getTables()) {
                    if ("all".equalsIgnoreCase(tableName)) {
                        log.info("  Skipping 'all' - not implemented in MVP");
                        continue;
                    }

                    MigrationResult result = migrateTable(
                        config,
                        task.getDatabase(),
                        tableName,
                        stateManager
                    );

                    if (!result.isSuccess()) {
                        overallSuccess = false;
                        if (!config.getMigration().getExecution().isContinueOnFailure()) {
                            log.error("Stopping due to failure (continueOnFailure=false)");
                            break;
                        }
                    }
                }
            }

            // 4. Mark completion
            stateManager.markCompleted();

            // 5. Exit with appropriate code
            if (overallSuccess) {
                log.info("=== Migration Completed Successfully ===");
                System.exit(0);
            } else {
                log.warn("=== Migration Completed with Failures ===");
                System.exit(1);
            }

        } catch (Exception e) {
            log.error("Migration failed with error", e);
            System.exit(2);
        }
    }

    private static AppConfig loadConfig(String configPath) throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        AppConfig config = mapper.readValue(new File(configPath), AppConfig.class);
        validateConfig(config);
        return config;
    }

    private static void validateConfig(AppConfig config) {
        if (config.getClusters() == null) {
            throw new IllegalArgumentException("Missing 'clusters' in config");
        }
        if (config.getClusters().getSource() == null) {
            throw new IllegalArgumentException("Missing 'clusters.source' in config");
        }
        if (config.getClusters().getTarget() == null) {
            throw new IllegalArgumentException("Missing 'clusters.target' in config");
        }
        if (config.getMigration() == null) {
            throw new IllegalArgumentException("Missing 'migration' in config");
        }
        log.info("Configuration validated successfully");
    }

    private static MigrationResult migrateTable(
            AppConfig config,
            String database,
            String tableName,
            JsonStateManager stateManager) {

        log.info("  Migrating table: {}.{}", database, tableName);
        stateManager.updateTableStatus(database, tableName, MigrationStatus.DATA_COPYING);

        try {
            // Build source and target paths
            ClusterConfig source = config.getClusters().getSource();
            ClusterConfig target = config.getClusters().getTarget();

            String sourcePath = source.getHdfs().getFullPath("/warehouse/tablespace/external/hive/" + database + ".db/" + tableName);
            String targetPath = target.getHdfs().getFullPath("/warehouse/tablespace/external/hive/" + database + ".db/" + tableName);

            log.info("    Source: {}", sourcePath);
            log.info("    Target: {}", targetPath);

            // Execute DistCp
            DistCpExecutor executor = new DistCpExecutor(
                config.getMigration().getDistcp());
            DistCpExecutor.ExecutionResult execResult = executor.execute(sourcePath, targetPath);

            if (execResult.isSuccess()) {
                stateManager.updateTableStatus(database, tableName, MigrationStatus.COMPLETED);
                return MigrationResult.builder()
                    .database(database)
                    .table(tableName)
                    .status(MigrationStatus.COMPLETED)
                    .build();
            } else {
                stateManager.updateTableStatus(database, tableName, MigrationStatus.FAILED);
                return MigrationResult.builder()
                    .database(database)
                    .table(tableName)
                    .status(MigrationStatus.FAILED)
                    .error("DISTCP_" + execResult.getExitCode(), execResult.getOutput())
                    .build();
            }

        } catch (Exception e) {
            log.error("    Migration failed: {}", e.getMessage());
            stateManager.updateTableStatus(database, tableName, MigrationStatus.FAILED);
            return MigrationResult.builder()
                .database(database)
                .table(tableName)
                .status(MigrationStatus.FAILED)
                .error("EXCEPTION", e.getMessage())
                .build();
        }
    }
}