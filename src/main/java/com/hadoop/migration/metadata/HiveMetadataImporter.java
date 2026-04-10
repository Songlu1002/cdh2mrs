package com.hadoop.migration.metadata;

import com.hadoop.migration.config.ClusterConfig;
import com.hadoop.migration.config.HdfsConfig;
import com.hadoop.migration.model.HiveColumn;
import com.hadoop.migration.model.TableMetadata;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveMetadataImporter {
    private static final Logger log = LoggerFactory.getLogger(HiveMetadataImporter.class);

    private final ClusterConfig clusterConfig;
    private HiveMetaStoreClient hmsClient;
    private boolean connected = false;

    public HiveMetadataImporter(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
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
                // Use configurable HMS thrift port
                int metastorePort = hdfs.getMetastorePort() > 0 ? hdfs.getMetastorePort() : 9083;
                conf.set("hive.metastore.uris", "thrift://" + hdfs.getNamenode() + ":" + metastorePort);
            }
            return new HiveMetaStoreClient(conf);
        } catch (Exception e) {
            log.error("Failed to create HMS client for {}", clusterConfig.getName(), e);
            throw new RuntimeException("Failed to connect to HMS: " + e.getMessage(), e);
        }
    }

    public void createTable(TableMetadata metadata) {
        try {
            if (tableExists(metadata.getDatabase(), metadata.getTableName())) {
                log.warn("Table {}.{} already exists, skipping creation",
                    metadata.getDatabase(), metadata.getTableName());
                return;
            }

            Table table = convertToHiveTable(metadata);
            getHmsClient().createTable(table);
            log.info("Successfully created table {}.{}", metadata.getDatabase(), metadata.getTableName());
        } catch (Exception e) {
            log.error("Failed to create table {}.{}", metadata.getDatabase(), metadata.getTableName(), e);
            throw new RuntimeException("Failed to create table: " + e.getMessage(), e);
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

    public void createDatabase(String dbName, String description) {
        try {
            // First check if database already exists (more reliable than catching exception)
            try {
                getHmsClient().getDatabase(dbName);
                log.info("Database {} already exists", dbName);
                return;
            } catch (Exception e) {
                // Database doesn't exist, proceed to create
            }

            org.apache.hadoop.hive.metastore.api.Database db =
                new org.apache.hadoop.hive.metastore.api.Database();
            db.setName(dbName);
            db.setDescription(description != null ? description : "");
            getHmsClient().createDatabase(db);
            log.info("Created database: {}", dbName);
        } catch (Exception e) {
            // Fallback: check if error indicates database already exists
            String msg = e.getMessage();
            if (msg != null && (msg.contains("already exists") || msg.contains("AlreadyExistsException"))) {
                log.info("Database {} already exists", dbName);
            } else {
                log.error("Failed to create database {}", dbName, e);
                throw new RuntimeException("Failed to create database: " + e.getMessage(), e);
            }
        }
    }

    private Table convertToHiveTable(TableMetadata metadata) {
        Table table = new Table();
        table.setDbName(metadata.getDatabase());
        table.setTableName(metadata.getTableName());
        table.setTableType(metadata.getTableType());

        StorageDescriptor sd = new StorageDescriptor();
        sd.setLocation(metadata.getLocation());
        sd.setInputFormat(metadata.getInputFormat());
        sd.setOutputFormat(metadata.getOutputFormat());

        if (metadata.getSerdeClass() != null) {
            SerDeInfo serdeInfo = new SerDeInfo();
            serdeInfo.setSerializationLib(metadata.getSerdeClass());
            sd.setSerdeInfo(serdeInfo);
        }

        // Convert columns
        List<FieldSchema> columns = new ArrayList<>();
        for (HiveColumn col : metadata.getColumns()) {
            columns.add(new FieldSchema(col.getName(), col.getType(),
                col.getComment() != null ? col.getComment() : ""));
        }
        sd.setCols(columns);

        // Convert partition columns
        List<FieldSchema> partitionKeys = new ArrayList<>();
        for (HiveColumn partCol : metadata.getPartitionColumns()) {
            partitionKeys.add(new FieldSchema(partCol.getName(), partCol.getType(),
                partCol.getComment() != null ? partCol.getComment() : ""));
        }
        table.setPartitionKeys(partitionKeys);

        table.setSd(sd);

        // Set table properties
        Map<String, String> props = metadata.getTableProperties();
        if (props != null && !props.isEmpty()) {
            table.setParameters(new HashMap<>(props));
        }

        return table;
    }

    public void close() {
        if (hmsClient != null) {
            hmsClient.close();
        }
    }
}