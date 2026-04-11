package com.hadoop.migration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.hadoop.migration.auth.KerberosAuthenticator;
import com.hadoop.migration.config.AppConfig;
import com.hadoop.migration.config.ClusterConfig;
import com.hadoop.migration.config.ExecutionConfig;
import com.hadoop.migration.config.KerberosConfig;
import com.hadoop.migration.config.MetadataConfig;
import com.hadoop.migration.config.MigrationTask;
import com.hadoop.migration.executor.DistCpExecutor;
import com.hadoop.migration.metadata.CompatibilityTransformer;
import com.hadoop.migration.metadata.HiveMetadataExtractor;
import com.hadoop.migration.metadata.HiveMetadataImporter;
import com.hadoop.migration.model.MigrationResult;
import com.hadoop.migration.model.MigrationStatus;
import com.hadoop.migration.model.TableMetadata;
import com.hadoop.migration.report.ReportGenerator;
import com.hadoop.migration.state.JsonStateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

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
        System.out.println("  bin\\migration-tool.bat --config <config-file>");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --config <file>   Path to configuration YAML file");
        System.out.println("  --verbose         Enable verbose output");
        System.out.println("  --help, -h        Show this help message");
        System.out.println();
        System.out.println("Example:");
        System.out.println("  bin\\migration-tool.bat --config conf\\config.yaml");
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

            // 3. Authenticate to both clusters (if Kerberos is enabled)
            if (!authenticateClusters(config)) {
                log.error("Cluster authentication failed");
                System.exit(2);
            }

            // 4. Determine if metadata migration is enabled
            MetadataConfig metadataConfig = config.getMigration().getMetadata();
            boolean useMetadataMigration = (metadataConfig != null && metadataConfig.isAutoConvert());

            if (useMetadataMigration) {
                log.info("Metadata migration enabled - will extract and transform table metadata");
            } else {
                log.info("Metadata migration disabled - using data-only migration");
            }

            // 5. Execute migration for each task
            boolean overallSuccess = true;
            int failedTaskCount = 0;
            HiveMetadataExtractor tableLister = null;

            // Create metadata extractor for listing tables when "all" is specified
            // (needed regardless of metadata migration mode)
            boolean needsTableLister = config.getMigration().getTasks().stream()
                .anyMatch(task -> task.isMigrateAllTables());
            if (needsTableLister) {
                tableLister = new HiveMetadataExtractor(config.getClusters().getSource());
            }

            try {
                for (MigrationTask task : config.getMigration().getTasks()) {
                    log.info("Processing database: {}", task.getDatabase());

                    if (task.getTables() == null) {
                        log.warn("No tables specified for database: {}", task.getDatabase());
                        continue;
                    }

                    // Resolve "all" to actual table names
                    List<String> tablesToMigrate = resolveTables(task, tableLister);
                    if (tablesToMigrate.isEmpty()) {
                        log.warn("No tables to migrate for database: {}", task.getDatabase());
                        continue;
                    }

                    log.info("  Tables to migrate: {}", tablesToMigrate.size());
                    int tableIndex = 0;
                    int skippedCount = 0;
                    for (String tableName : tablesToMigrate) {
                        tableIndex++;

                        // Skip already completed tables (resume functionality)
                        if (stateManager.isTableCompleted(task.getDatabase(), tableName)) {
                            log.info("  [{}/{}] Skipping already completed table: {}.{}",
                                tableIndex, tablesToMigrate.size(), task.getDatabase(), tableName);
                            skippedCount++;
                            continue;
                        }

                        log.info("  [{}/{}] Migrating table: {}.{}", tableIndex, tablesToMigrate.size(), task.getDatabase(), tableName);

                        MigrationResult result;
                        if (useMetadataMigration) {
                            result = migrateTableWithMetadata(
                                config,
                                task.getDatabase(),
                                tableName,
                                stateManager
                            );
                        } else {
                            result = migrateTable(
                                config,
                                task.getDatabase(),
                                tableName,
                                stateManager
                            );
                        }

                        if (!result.isSuccess()) {
                            overallSuccess = false;
                            failedTaskCount++;
                            ExecutionConfig execConfig = config.getMigration().getExecution();
                            if (execConfig != null) {
                                if (!execConfig.isContinueOnFailure()) {
                                    log.error("Stopping due to failure (continueOnFailure=false)");
                                    break;
                                }
                                if (execConfig.getMaxFailedTasks() > 0 && failedTaskCount >= execConfig.getMaxFailedTasks()) {
                                    log.error("Stopping due to reaching maxFailedTasks limit ({})", execConfig.getMaxFailedTasks());
                                    break;
                                }
                            }
                        }
                    }
                    if (skippedCount > 0) {
                        log.info("  Skipped {} already completed tables in database {}", skippedCount, task.getDatabase());
                    }
                }
            } finally {
                if (tableLister != null) {
                    try {
                        tableLister.close();
                    } catch (Exception e) {
                        log.warn("Error closing table lister", e);
                    }
                }
            }

            // 6. Generate migration report (before marking completion so report captures final state)
            log.info("Generating migration report...");
            ReportGenerator reportGenerator = new ReportGenerator(config.getMigration().getOutput().getReportDir());
            String reportPath = reportGenerator.generateReport(
                stateManager.getState(),
                config.getClusters().getSource().getName(),
                config.getClusters().getTarget().getName()
            );
            if (reportPath != null) {
                log.info("Migration report: {}", reportPath);
            }

            // 7. Mark completion
            stateManager.markCompleted();

            // 8. Exit with appropriate code
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

    private static boolean authenticateClusters(AppConfig config) {
        log.info("Authenticating to clusters...");

        ClusterConfig source = config.getClusters().getSource();
        ClusterConfig target = config.getClusters().getTarget();

        try {
            // Authenticate to source cluster if Kerberos is enabled
            KerberosConfig sourceKerberos = source.getKerberos();
            if (sourceKerberos != null && sourceKerberos.isEnabled()) {
                log.info("Authenticating to source cluster {} with Kerberos", source.getName());
                KerberosAuthenticator.authenticate(sourceKerberos);
            } else {
                log.info("Source cluster {} does not use Kerberos authentication", source.getName());
            }

            // Authenticate to target cluster if Kerberos is enabled
            KerberosConfig targetKerberos = target.getKerberos();
            if (targetKerberos != null && targetKerberos.isEnabled()) {
                log.info("Authenticating to target cluster {} with Kerberos", target.getName());
                KerberosAuthenticator.authenticate(targetKerberos);
            } else {
                log.info("Target cluster {} does not use Kerberos authentication", target.getName());
            }

            log.info("Cluster authentication completed successfully");
            return true;
        } catch (Exception e) {
            log.error("Cluster authentication failed: {}", e.getMessage());
            return false;
        }
    }

    private static List<String> resolveTables(MigrationTask task, HiveMetadataExtractor tableLister) {
        if (task.isMigrateAllTables()) {
            log.info("    'all' specified - listing tables from source HMS...");
            try {
                List<String> allTables = tableLister.listTables(task.getDatabase());
                log.info("    Found {} tables in database {}", allTables.size(), task.getDatabase());
                return allTables;
            } catch (Exception e) {
                log.error("    Failed to list tables from HMS: {}", e.getMessage());
                return List.of();
            }
        } else {
            List<String> tables = task.getTables();
            return tables != null ? tables : List.of();
        }
    }

    private static MigrationResult migrateTable(
            AppConfig config,
            String database,
            String tableName,
            JsonStateManager stateManager) {

        log.info("  Migrating table: {}.{}", database, tableName);
        stateManager.updateTableStatus(database, tableName, MigrationStatus.METADATA_EXTRACTING);

        ClusterConfig source = config.getClusters().getSource();
        ClusterConfig target = config.getClusters().getTarget();
        MetadataConfig metadataConfig = config.getMigration().getMetadata();

        // Get source and target namenodes for location rewriting
        String sourceNamenode = source.getHdfs().getProtocol() + "://" + source.getHdfs().getNamenode() + ":" + source.getHdfs().getPort();
        String targetNamenode = target.getHdfs().getProtocol() + "://" + target.getHdfs().getNamenode() + ":" + target.getHdfs().getPort();

        HiveMetadataExtractor extractor = null;
        DistCpExecutor executor = null;

        try {
            // Step 1: Extract metadata to get real location (needed for both table types)
            log.info("    Extracting metadata to determine real location...");
            extractor = new HiveMetadataExtractor(source);
            TableMetadata sourceMetadata = extractor.extractTableMetadata(database, tableName);

            if (sourceMetadata.isView()) {
                log.warn("    View {}.{} detected - views require manual migration", database, tableName);
                stateManager.updateTableStatus(database, tableName, MigrationStatus.FAILED);
                return MigrationResult.builder()
                    .database(database)
                    .table(tableName)
                    .status(MigrationStatus.FAILED)
                    .error("VIEW", "Views cannot be migrated automatically")
                    .build();
            }

            log.info("    Table type: {}, location: {}", sourceMetadata.getTableType(), sourceMetadata.getLocation());

            // Step 2: Transform location for target
            String sourcePath = sourceMetadata.getLocation();
            String targetPath;

            if (sourcePath != null && sourcePath.startsWith(sourceNamenode)) {
                targetPath = targetNamenode + sourcePath.substring(sourceNamenode.length());
            } else {
                // Fallback to configured path if location doesn't match expected pattern
                String externalPath = config.getMigration().getDistcp().getExternalTablePath();
                targetPath = target.getHdfs().getFullPath(externalPath + database + ".db/" + tableName);
                log.warn("    Source location {} does not match source namenode pattern {}, using fallback target path: {}",
                    sourcePath, sourceNamenode, targetPath);
            }

            // Step 3: Execute DistCp
            stateManager.updateTableStatus(database, tableName, MigrationStatus.DATA_COPYING);
            log.info("    Copying data with DistCp...");
            log.info("      Source: {}", sourcePath);
            log.info("      Target: {}", targetPath);

            executor = new DistCpExecutor(config.getMigration().getDistcp());
            DistCpExecutor.ExecutionResult execResult = executor.execute(sourcePath, targetPath);

            if (execResult.isSuccess()) {
                stateManager.updateTableStatus(database, tableName, MigrationStatus.COMPLETED);
                return MigrationResult.builder()
                    .database(database)
                    .table(tableName)
                    .status(MigrationStatus.COMPLETED)
                    .build();
            } else {
                log.error("    DistCp failed with exit code: {}", execResult.getExitCode());
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
        } finally {
            if (executor != null) {
                try {
                    executor.close();
                } catch (Exception e) {
                    log.warn("Error closing DistCpExecutor", e);
                }
            }
            if (extractor != null) {
                try {
                    extractor.close();
                } catch (Exception e) {
                    log.warn("Error closing metadata extractor", e);
                }
            }
        }
    }

    private static MigrationResult migrateTableWithMetadata(
            AppConfig config,
            String database,
            String tableName,
            JsonStateManager stateManager) {

        log.info("  Migrating table with metadata: {}.{}", database, tableName);

        ClusterConfig source = config.getClusters().getSource();
        ClusterConfig target = config.getClusters().getTarget();
        MetadataConfig metadataConfig = config.getMigration().getMetadata();

        // Get source and target namenodes for location rewriting
        String sourceNamenode = source.getHdfs().getProtocol() + "://" + source.getHdfs().getNamenode() + ":" + source.getHdfs().getPort();
        String targetNamenode = target.getHdfs().getProtocol() + "://" + target.getHdfs().getNamenode() + ":" + target.getHdfs().getPort();

        HiveMetadataExtractor extractor = null;
        HiveMetadataImporter importer = null;
        DistCpExecutor executor = null;
        String targetPath = null;
        boolean dataCopied = false;

        try {
            // Step 1: Extract metadata from source
            stateManager.updateTableStatus(database, tableName, MigrationStatus.METADATA_MIGRATING);
            log.info("    Extracting metadata from source...");

            extractor = new HiveMetadataExtractor(source);
            TableMetadata sourceMetadata = extractor.extractTableMetadata(database, tableName);

            if (sourceMetadata.isView()) {
                log.warn("    View {}.{} detected - views require manual migration", database, tableName);
                stateManager.updateTableStatus(database, tableName, MigrationStatus.FAILED);
                return MigrationResult.builder()
                    .database(database)
                    .table(tableName)
                    .status(MigrationStatus.FAILED)
                    .error("VIEW", "Views cannot be migrated automatically")
                    .build();
            }

            log.info("    Extracted metadata: type={}, location={}",
                sourceMetadata.getTableType(), sourceMetadata.getLocation());

            // Step 2: Transform metadata for Hive 3.x compatibility
            log.info("    Transforming metadata for Hive 3.x compatibility...");
            CompatibilityTransformer transformer = new CompatibilityTransformer(
                metadataConfig, sourceNamenode, targetNamenode);
            TableMetadata transformedMetadata = transformer.transform(sourceMetadata);
            log.info("    Transformed metadata: location={}", transformedMetadata.getLocation());

            // Step 3: Execute DistCp for data copy
            // Use actual source location from metadata, target location is rewritten
            String sourcePath = sourceMetadata.getLocation();
            targetPath = transformedMetadata.getLocation();

            stateManager.updateTableStatus(database, tableName, MigrationStatus.DATA_COPYING);
            log.info("    Copying data with DistCp...");
            log.info("      Source: {}", sourcePath);
            log.info("      Target: {}", targetPath);

            executor = new DistCpExecutor(config.getMigration().getDistcp());
            DistCpExecutor.ExecutionResult execResult = executor.execute(sourcePath, targetPath);

            if (!execResult.isSuccess()) {
                log.error("    DistCp failed with exit code: {}", execResult.getExitCode());
                stateManager.updateTableStatus(database, tableName, MigrationStatus.FAILED);
                return MigrationResult.builder()
                    .database(database)
                    .table(tableName)
                    .status(MigrationStatus.FAILED)
                    .error("DISTCP_" + execResult.getExitCode(), execResult.getOutput())
                    .build();
            }

            dataCopied = true;
            log.info("    Data copy completed successfully");

            // Step 4: Import metadata to target
            stateManager.updateTableStatus(database, tableName, MigrationStatus.METADATA_MIGRATING);
            log.info("    Importing metadata to target...");

            importer = new HiveMetadataImporter(target);

            // Ensure database exists
            importer.createDatabase(database, null);

            // Create the table
            importer.createTable(transformedMetadata);

            // Verify table was created
            if (importer.tableExists(database, tableName)) {
                log.info("    Successfully created table {}.{}", database, tableName);
                stateManager.updateTableStatus(database, tableName, MigrationStatus.COMPLETED);
                return MigrationResult.builder()
                    .database(database)
                    .table(tableName)
                    .status(MigrationStatus.COMPLETED)
                    .build();
            } else {
                // Table creation failed after data copy - need to clean up
                log.error("    Table {}.{} was not created, cleaning up data", database, tableName);
                cleanupOnFailure(executor, targetPath);
                stateManager.updateTableStatus(database, tableName, MigrationStatus.FAILED);
                return MigrationResult.builder()
                    .database(database)
                    .table(tableName)
                    .status(MigrationStatus.FAILED)
                    .error("IMPORT_FAILED", "Table creation verification failed")
                    .build();
            }

        } catch (UnsupportedOperationException e) {
            // Handle unsupported features (views, UNIONTYPE, etc.)
            log.error("    Unsupported operation: {}", e.getMessage());
            stateManager.updateTableStatus(database, tableName, MigrationStatus.FAILED);
            return MigrationResult.builder()
                .database(database)
                .table(tableName)
                .status(MigrationStatus.FAILED)
                .error("UNSUPPORTED", e.getMessage())
                .build();

        } catch (Exception e) {
            log.error("    Migration failed: {}", e.getMessage());
            // If data was copied but migration failed, clean up orphaned data
            if (dataCopied && targetPath != null) {
                cleanupOnFailure(executor, targetPath);
            }
            stateManager.updateTableStatus(database, tableName, MigrationStatus.FAILED);
            return MigrationResult.builder()
                .database(database)
                .table(tableName)
                .status(MigrationStatus.FAILED)
                .error("EXCEPTION", e.getMessage())
                .build();

        } finally {
            // Clean up resources
            if (executor != null) {
                try {
                    executor.close();
                } catch (Exception e) {
                    log.warn("Error closing DistCpExecutor", e);
                }
            }
            if (extractor != null) {
                try {
                    extractor.close();
                } catch (Exception e) {
                    log.warn("Error closing metadata extractor", e);
                }
            }
            if (importer != null) {
                try {
                    importer.close();
                } catch (Exception e) {
                    log.warn("Error closing metadata importer", e);
                }
            }
        }
    }

    /**
     * Clean up orphaned data on target cluster when migration fails after data copy.
     */
    private static void cleanupOnFailure(DistCpExecutor executor, String targetPath) {
        if (executor != null && targetPath != null) {
            try {
                log.info("    Attempting to clean up orphaned data at: {}", targetPath);
                boolean cleanupSuccess = executor.cleanupTarget(targetPath);
                if (cleanupSuccess) {
                    log.info("    Successfully cleaned up orphaned data");
                } else {
                    log.warn("    Failed to clean up orphaned data - manual cleanup may be required for: {}", targetPath);
                }
            } catch (Exception e) {
                log.error("    Exception during cleanup: {}", e.getMessage());
            }
        }
    }
}