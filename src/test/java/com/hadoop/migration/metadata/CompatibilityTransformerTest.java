package com.hadoop.migration.metadata;

import com.hadoop.migration.config.MetadataConfig;
import com.hadoop.migration.model.HiveColumn;
import com.hadoop.migration.model.TableMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.Arrays;
import static org.junit.jupiter.api.Assertions.*;

class CompatibilityTransformerTest {

    private CompatibilityTransformer transformer;
    private MetadataConfig config;

    @BeforeEach
    void setUp() {
        config = new MetadataConfig();
        transformer = new CompatibilityTransformer(config,
            "hdfs://cdh-nn:8020", "hdfs://mrs-nn:8020");
    }

    @Test
    void testRewriteHdfsLocation() {
        TableMetadata source = TableMetadata.builder()
            .database("db1")
            .tableName("table1")
            .location("hdfs://cdh-nn:8020/warehouse/db1.db/table1")
            .build();

        TableMetadata result = transformer.transform(source);

        assertEquals("hdfs://mrs-nn:8020/warehouse/db1.db/table1", result.getLocation());
    }

    @Test
    void testAddTransactionalProperty() {
        TableMetadata source = TableMetadata.builder()
            .database("db1")
            .tableName("table1")
            .tableType("MANAGED_TABLE")
            .build();

        TableMetadata result = transformer.transform(source);

        assertEquals("false", result.getTableProperties().get("transactional"));
    }

    @Test
    void testUpgradeBucketingVersion() {
        TableMetadata source = TableMetadata.builder()
            .database("db1")
            .tableName("table1")
            .tableProperties(new java.util.HashMap<>())
            .build();
        source.getTableProperties().put("bucket_count", "10");

        TableMetadata result = transformer.transform(source);

        assertEquals("2", result.getTableProperties().get("bucketing_version"));
    }

    @Test
    void testDetectUnionTypeColumn() {
        TableMetadata source = TableMetadata.builder()
            .database("db1")
            .tableName("table1")
            .columns(Arrays.asList(
                HiveColumn.builder().name("col1").type("string").build(),
                HiveColumn.builder().name("union_col").type("UNIONTYPE<string,int>").build()
            ))
            .build();

        boolean hasUnsupported = transformer.hasUnsupportedFeatures(source);

        assertTrue(hasUnsupported);
    }
}