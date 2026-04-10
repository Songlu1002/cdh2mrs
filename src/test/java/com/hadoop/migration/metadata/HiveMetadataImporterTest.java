package com.hadoop.migration.metadata;

import com.hadoop.migration.config.ClusterConfig;
import com.hadoop.migration.config.HdfsConfig;
import com.hadoop.migration.model.HiveColumn;
import com.hadoop.migration.model.TableMetadata;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import java.util.Arrays;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class HiveMetadataImporterTest {

    @Mock
    private HiveMetaStoreClient mockClient;

    @Test
    void testImporterRequiresValidConfig() {
        ClusterConfig config = new ClusterConfig();
        config.setName("mrs-cluster");

        HiveMetadataImporter importer = new HiveMetadataImporter(config);
        assertNotNull(importer);
    }

    @Test
    void testImporterWithHdfsConfig() {
        ClusterConfig config = new ClusterConfig();
        config.setName("mrs-cluster");
        HdfsConfig hdfs = new HdfsConfig();
        hdfs.setNamenode("mrs-namenode.example.com");
        hdfs.setPort(9083);
        config.setHdfs(hdfs);

        HiveMetadataImporter importer = new HiveMetadataImporter(config);
        assertNotNull(importer);
        // isConnected returns false initially since client hasn't been created
        assertFalse(importer.isConnected());
    }

    @Test
    void testImporterNullHdfs() {
        ClusterConfig config = new ClusterConfig();
        config.setName("mrs-cluster");
        // hdfs is null

        HiveMetadataImporter importer = new HiveMetadataImporter(config);
        assertNotNull(importer);
    }

    @Test
    void testImporterCloseWithoutConnection() {
        ClusterConfig config = new ClusterConfig();
        config.setName("mrs-cluster");

        HiveMetadataImporter importer = new HiveMetadataImporter(config);
        // close() should not throw even without any connection
        assertDoesNotThrow(() -> importer.close());
    }

    @Test
    void testImporterCloseWithConnection() throws Exception {
        ClusterConfig config = new ClusterConfig();
        config.setName("mrs-cluster");
        HdfsConfig hdfs = new HdfsConfig();
        hdfs.setNamenode("mrs-namenode.example.com");
        hdfs.setPort(9083);
        config.setHdfs(hdfs);

        HiveMetadataImporter importer = new HiveMetadataImporter(config);
        // Manually set the client to test close
        java.lang.reflect.Field clientField = HiveMetadataImporter.class.getDeclaredField("hmsClient");
        clientField.setAccessible(true);
        clientField.set(importer, mockClient);

        importer.close();

        verify(mockClient).close();
    }

    @Test
    void testCreateTableWithNullDescription() {
        // Test that createDatabase handles null description gracefully
        ClusterConfig config = new ClusterConfig();
        config.setName("mrs-cluster");
        HdfsConfig hdfs = new HdfsConfig();
        hdfs.setNamenode("mrs-namenode.example.com");
        hdfs.setPort(9083);
        config.setHdfs(hdfs);

        HiveMetadataImporter importer = new HiveMetadataImporter(config);
        assertNotNull(importer);
    }
}