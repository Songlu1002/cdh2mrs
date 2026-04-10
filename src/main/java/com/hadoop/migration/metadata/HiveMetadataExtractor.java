package com.hadoop.migration.metadata;

import com.hadoop.migration.config.ClusterConfig;
import com.hadoop.migration.config.HdfsConfig;
import com.hadoop.migration.model.HiveColumn;
import com.hadoop.migration.model.TableMetadata;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveMetadataExtractor {
    private static final Logger log = LoggerFactory.getLogger(HiveMetadataExtractor.class);

    private final ClusterConfig clusterConfig;
    private HiveMetaStoreClient hmsClient;
    private final String hiveVersion;
    private boolean connected = false;

    public HiveMetadataExtractor(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
        this.hiveVersion = clusterConfig.getHiveVersion();
        // Lazy initialization - don't connect in constructor
        // Connection will be established on first use
    }

    /**
     * Gets or creates the HMS client. Lazy initialization.
     */
    private synchronized HiveMetaStoreClient getHmsClient() {
        if (hmsClient == null) {
            hmsClient = createHmsClient();
            connected = true;
        }
        return hmsClient;
    }

    /**
     * Returns true if successfully connected to HMS.
     * Note: This may return false during client creation even if connection will succeed.
     */
    public boolean isConnected() {
        return hmsClient != null;
    }

    private HiveMetaStoreClient createHmsClient() {
        try {
            org.apache.hadoop.hive.conf.HiveConf conf = new org.apache.hadoop.hive.conf.HiveConf();
            HdfsConfig hdfs = clusterConfig.getHdfs();
            if (hdfs != null) {
                // Use HMS thrift port 9083
                conf.set("hive.metastore.uris", "thrift://" + hdfs.getNamenode() + ":9083");
            }
            return new HiveMetaStoreClient(conf);
        } catch (Exception e) {
            log.error("Failed to create HMS client for {}", clusterConfig.getName(), e);
            throw new RuntimeException("Failed to connect to HMS: " + e.getMessage(), e);
        }
    }

    public TableMetadata extractTableMetadata(String dbName, String tableName) {
        try {
            Table table = getHmsClient().getTable(dbName, tableName);
            return convertToTableMetadata(table);
        } catch (Exception e) {
            log.error("Failed to extract metadata for {}.{}", dbName, tableName, e);
            throw new RuntimeException("Failed to extract metadata: " + e.getMessage(), e);
        }
    }

    public List<String> listTables(String dbName) {
        try {
            return getHmsClient().getAllTables(dbName);
        } catch (Exception e) {
            log.error("Failed to list tables in {}", dbName, e);
            throw new RuntimeException("Failed to list tables: " + e.getMessage(), e);
        }
    }

    public List<String> listDatabases() {
        try {
            return getHmsClient().getAllDatabases();
        } catch (Exception e) {
            log.error("Failed to list databases", e);
            throw new RuntimeException("Failed to list databases: " + e.getMessage(), e);
        }
    }

    public boolean tableExists(String dbName, String tableName) {
        try {
            return getHmsClient().tableExists(dbName, tableName);
        } catch (Exception e) {
            log.warn("Error checking table existence {}.{}: {}", dbName, tableName, e.getMessage());
            return false;
        }
    }

    private TableMetadata convertToTableMetadata(Table table) {
        TableMetadata.Builder builder = TableMetadata.builder()
            .database(table.getDbName())
            .tableName(table.getTableName())
            .tableType(table.getTableType());

        StorageDescriptor sd = table.getSd();
        if (sd != null) {
            builder.location(sd.getLocation())
                   .inputFormat(sd.getInputFormat())
                   .outputFormat(sd.getOutputFormat());

            if (sd.getSerdeInfo() != null) {
                builder.serdeClass(sd.getSerdeInfo().getSerializationLib());
            }

            // Convert columns
            List<HiveColumn> columns = new ArrayList<>();
            if (sd.getCols() != null) {
                int pos = 0;
                for (FieldSchema col : sd.getCols()) {
                    columns.add(HiveColumn.builder()
                        .name(col.getName())
                        .type(col.getType())
                        .comment(col.getComment() != null ? col.getComment() : "")
                        .position(pos++)
                        .isPartition(false)
                        .build());
                }
                builder.columns(columns);
            }

            // Convert partition columns
            List<HiveColumn> partitionCols = new ArrayList<>();
            if (table.getPartitionKeys() != null) {
                int pos = 0;
                for (FieldSchema partCol : table.getPartitionKeys()) {
                    partitionCols.add(HiveColumn.builder()
                        .name(partCol.getName())
                        .type(partCol.getType())
                        .comment(partCol.getComment() != null ? partCol.getComment() : "")
                        .position(pos++)
                        .isPartition(true)
                        .build());
                }
                builder.partitionColumns(partitionCols);
            }
        }

        // Convert table properties
        Map<String, String> params = table.getParameters();
        if (params != null) {
            builder.tableProperties(new HashMap<>(params));
        }

        // Check if this is a view
        if (table.getViewOriginalText() != null && !table.getViewOriginalText().isEmpty()) {
            builder.isView(true)
                   .viewOriginalText(table.getViewOriginalText());
        }

        return builder.build();
    }

    public void close() {
        if (hmsClient != null) {
            hmsClient.close();
        }
    }
}
