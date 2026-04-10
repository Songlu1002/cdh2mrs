package com.hadoop.migration.metadata;

import com.hadoop.migration.config.MetadataConfig;
import com.hadoop.migration.model.HiveColumn;
import com.hadoop.migration.model.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class CompatibilityTransformer {
    private static final Logger log = LoggerFactory.getLogger(CompatibilityTransformer.class);

    private static final Pattern UNIONTYPE_PATTERN = Pattern.compile("UNIONTYPE", Pattern.CASE_INSENSITIVE);

    private final MetadataConfig config;
    private final String sourceNamenode;
    private final String targetNamenode;

    public CompatibilityTransformer(MetadataConfig config, String sourceNamenode, String targetNamenode) {
        this.config = config;
        this.sourceNamenode = sourceNamenode;
        this.targetNamenode = targetNamenode;
    }

    public TableMetadata transform(TableMetadata source) {
        log.info("Transforming metadata for {}.{}", source.getDatabase(), source.getTableName());

        TableMetadata transformed = cloneTableMetadata(source);

        // 1. Rewrite HDFS locations
        if (config.isRewriteLocations() && transformed.getLocation() != null) {
            transformed.setLocation(rewriteLocation(transformed.getLocation()));
        }

        // 2. Add transactional=false for managed tables
        if (config.isAddTransactionalProp() && "MANAGED_TABLE".equals(transformed.getTableType())) {
            transformed.getTableProperties().put("transactional", "false");
        }

        // 3. Clear statistics
        if (config.isClearStatistics()) {
            clearStatistics(transformed);
        }

        // 4. Upgrade bucketing version
        if (config.isUpgradeBucketingVersion()) {
            upgradeBucketingVersion(transformed);
        }

        // 5. Check for unsupported features (views, UDFs, etc.)
        if (transformed.isView()) {
            handleView(transformed);
        }

        // 6. Check for UNIONTYPE columns
        if (hasUnsupportedFeatures(transformed)) {
            handleUnsupportedFeatures(transformed);
        }

        return transformed;
    }

    public boolean hasUnsupportedFeatures(TableMetadata table) {
        // Check for UNIONTYPE columns
        for (HiveColumn col : table.getColumns()) {
            if (col.getType() != null && UNIONTYPE_PATTERN.matcher(col.getType()).find()) {
                log.warn("Column {} has unsupported type: {}", col.getName(), col.getType());
                return true;
            }
        }
        for (HiveColumn col : table.getPartitionColumns()) {
            if (col.getType() != null && UNIONTYPE_PATTERN.matcher(col.getType()).find()) {
                log.warn("Partition column {} has unsupported type: {}", col.getName(), col.getType());
                return true;
            }
        }
        return false;
    }

    private String rewriteLocation(String location) {
        if (location == null || location.isEmpty()) {
            return location;
        }
        if (location.startsWith(sourceNamenode)) {
            String newLocation = location.replace(sourceNamenode, targetNamenode);
            log.debug("Rewrote location: {} -> {}", location, newLocation);
            return newLocation;
        }
        log.warn("Location {} does not match source pattern {}, keeping as-is", location, sourceNamenode);
        return location;
    }

    private void clearStatistics(TableMetadata table) {
        Map<String, String> props = table.getTableProperties();
        props.remove("numFiles");
        props.remove("numPartitions");
        props.remove("numRows");
        props.remove("rawDataSize");
        props.remove("totalSize");
        props.remove("COLUMN_STATS_ACCURATE");
        props.remove("numFilesErasureCoding");
    }

    private void upgradeBucketingVersion(TableMetadata table) {
        Map<String, String> props = table.getTableProperties();
        if (props.containsKey("bucket_count")) {
            props.put("bucketing_version", "2");
        }
    }

    private void handleView(TableMetadata view) {
        if (!config.isMigrateViews()) {
            switch (config.getUnsupportedAction()) {
                case "FAIL":
                    throw new UnsupportedOperationException(
                        "View " + view.getDatabase() + "." + view.getTableName() +
                        " cannot be migrated automatically. Set migrateViews=true or recreate manually.");
                case "WARN":
                    log.warn("View {}.{} will be skipped. DDL available for manual recreation.",
                        view.getDatabase(), view.getTableName());
                    if (config.isGenerateViewDdl()) {
                        String viewDdl = generateViewDdl(view);
                        // DDL is logged at INFO level above, caller can retrieve via CompatibilityTransformer
                    }
                    break;
                case "SKIP":
                default:
                    log.info("Skipping view {}.{}", view.getDatabase(), view.getTableName());
                    break;
            }
        }
    }

    private void handleUnsupportedFeatures(TableMetadata table) {
        switch (config.getUnsupportedAction()) {
            case "FAIL":
                throw new UnsupportedOperationException(
                    "Table " + table.getDatabase() + "." + table.getTableName() +
                    " contains unsupported features (UNIONTYPE columns). Manual conversion required.");
            case "WARN":
                log.warn("Table {}.{} contains unsupported features. Proceeding with caution.",
                    table.getDatabase(), table.getTableName());
                break;
            case "SKIP":
            default:
                log.info("Skipping table {}.{} due to unsupported features.",
                    table.getDatabase(), table.getTableName());
                break;
        }
    }

    /**
     * Generates CREATE VIEW DDL for the given view metadata.
     * @return the CREATE VIEW DDL string, or null if view has no columns defined
     */
    public String generateViewDdl(TableMetadata view) {
        if (view.getViewOriginalText() == null || view.getViewOriginalText().isEmpty()) {
            log.warn("View {}.{} has no view original text, cannot generate DDL",
                view.getDatabase(), view.getTableName());
            return null;
        }

        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE VIEW ")
           .append(view.getDatabase())
           .append(".")
           .append(view.getTableName());

        if (view.getColumns() != null && !view.getColumns().isEmpty()) {
            ddl.append(" (");
            for (int i = 0; i < view.getColumns().size(); i++) {
                if (i > 0) ddl.append(", ");
                ddl.append(view.getColumns().get(i).getName());
            }
            ddl.append(")");
        }

        ddl.append(" AS ")
           .append(view.getViewOriginalText());

        String ddlString = ddl.toString();
        log.info("Generated VIEW DDL for {}.{}:\n{}",
            view.getDatabase(), view.getTableName(), ddlString);
        return ddlString;
    }

    private TableMetadata cloneTableMetadata(TableMetadata source) {
        // Create a deep copy including HiveColumn objects
        TableMetadata clone = new TableMetadata();
        clone.setDatabase(source.getDatabase());
        clone.setTableName(source.getTableName());
        clone.setTableType(source.getTableType());
        clone.setLocation(source.getLocation());
        clone.setInputFormat(source.getInputFormat());
        clone.setOutputFormat(source.getOutputFormat());
        clone.setSerdeClass(source.getSerdeClass());

        // Deep copy columns to avoid shared references with source
        List<HiveColumn> columnsCopy = new ArrayList<>();
        for (HiveColumn col : source.getColumns()) {
            columnsCopy.add(HiveColumn.builder()
                .name(col.getName())
                .type(col.getType())
                .comment(col.getComment())
                .position(col.getPosition())
                .isPartition(col.isPartition())
                .build());
        }
        clone.setColumns(columnsCopy);

        // Deep copy partition columns
        List<HiveColumn> partitionColsCopy = new ArrayList<>();
        for (HiveColumn col : source.getPartitionColumns()) {
            partitionColsCopy.add(HiveColumn.builder()
                .name(col.getName())
                .type(col.getType())
                .comment(col.getComment())
                .position(col.getPosition())
                .isPartition(col.isPartition())
                .build());
        }
        clone.setPartitionColumns(partitionColsCopy);

        clone.setTableProperties(new HashMap<>(source.getTableProperties()));
        clone.setViewOriginalText(source.getViewOriginalText());
        clone.setView(source.isView());
        return clone;
    }
}