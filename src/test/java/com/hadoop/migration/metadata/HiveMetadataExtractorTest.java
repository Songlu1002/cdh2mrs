package com.hadoop.migration.metadata;

import com.hadoop.migration.config.ClusterConfig;
import com.hadoop.migration.config.HdfsConfig;
import com.hadoop.migration.model.TableMetadata;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class HiveMetadataExtractorTest {

    @Mock
    private HiveMetaStoreClient mockClient;

    @Test
    void testExtractorRequiresValidConfig() {
        ClusterConfig config = new ClusterConfig();
        config.setName("test-cluster");

        HiveMetadataExtractor extractor = new HiveMetadataExtractor(config);
        assertNotNull(extractor);
    }

    @Test
    void testExtractorWithHdfsConfig() {
        ClusterConfig config = new ClusterConfig();
        config.setName("test-cluster");
        HdfsConfig hdfs = new HdfsConfig();
        hdfs.setNamenode("namenode.example.com");
        hdfs.setPort(9083);
        config.setHdfs(hdfs);

        HiveMetadataExtractor extractor = new HiveMetadataExtractor(config);
        assertNotNull(extractor);
        // isConnected returns false initially since client hasn't been created
        assertFalse(extractor.isConnected());
    }

    @Test
    void testExtractorNullHdfs() {
        ClusterConfig config = new ClusterConfig();
        config.setName("test-cluster");
        // hdfs is null

        HiveMetadataExtractor extractor = new HiveMetadataExtractor(config);
        assertNotNull(extractor);
    }

    @Test
    void testExtractorCloseWithoutConnection() {
        ClusterConfig config = new ClusterConfig();
        config.setName("test-cluster");

        HiveMetadataExtractor extractor = new HiveMetadataExtractor(config);
        // close() should not throw even without any connection
        assertDoesNotThrow(() -> extractor.close());
    }

    @Test
    void testExtractorCloseWithConnection() throws Exception {
        ClusterConfig config = new ClusterConfig();
        config.setName("test-cluster");
        HdfsConfig hdfs = new HdfsConfig();
        hdfs.setNamenode("namenode.example.com");
        hdfs.setPort(9083);
        config.setHdfs(hdfs);

        HiveMetadataExtractor extractor = new HiveMetadataExtractor(config);
        // Manually set the client to test close
        java.lang.reflect.Field clientField = HiveMetadataExtractor.class.getDeclaredField("hmsClient");
        clientField.setAccessible(true);
        clientField.set(extractor, mockClient);

        extractor.close();

        verify(mockClient).close();
    }
}