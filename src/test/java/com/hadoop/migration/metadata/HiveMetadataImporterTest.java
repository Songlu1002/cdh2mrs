package com.hadoop.migration.metadata;

import com.hadoop.migration.config.ClusterConfig;
import com.hadoop.migration.model.TableMetadata;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class HiveMetadataImporterTest {

    @Test
    void testImporterRequiresValidConfig() {
        ClusterConfig config = new ClusterConfig();
        config.setName("mrs-cluster");

        HiveMetadataImporter importer = new HiveMetadataImporter(config);
        assertNotNull(importer);
    }
}