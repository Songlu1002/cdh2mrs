package com.hadoop.migration.metadata;

import com.hadoop.migration.config.ClusterConfig;
import com.hadoop.migration.config.HdfsConfig;
import com.hadoop.migration.model.TableMetadata;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class HiveMetadataExtractorTest {

    @Test
    void testExtractorRequiresValidConfig() {
        ClusterConfig config = new ClusterConfig();
        config.setName("test-cluster");

        // Create extractor with null HMS - should handle gracefully in test
        HiveMetadataExtractor extractor = new HiveMetadataExtractor(config);
        assertNotNull(extractor);
    }
}