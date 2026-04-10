package com.hadoop.migration.metadata;

import com.hadoop.migration.config.MetadataConfig;
import com.hadoop.migration.model.HiveColumn;
import com.hadoop.migration.model.TableMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.List;
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

    @Test
    void testRewriteLocationWithDifferentSourceNamenode() {
        // Test that location not matching source pattern is kept as-is
        TableMetadata source = TableMetadata.builder()
            .database("db1")
            .tableName("table1")
            .location("hdfs://other-nn:8020/path/to/table")
            .build();

        TableMetadata result = transformer.transform(source);

        // Location should be kept as-is since it doesn't match source pattern
        assertEquals("hdfs://other-nn:8020/path/to/table", result.getLocation());
    }

    @Test
    void testClearStatistics() {
        config.setClearStatistics(true);
        TableMetadata source = TableMetadata.builder()
            .database("db1")
            .tableName("table1")
            .tableProperties(new java.util.HashMap<>())
            .build();
        source.getTableProperties().put("numFiles", "10");
        source.getTableProperties().put("numRows", "1000");
        source.getTableProperties().put("totalSize", "1000000");
        source.getTableProperties().put("COLUMN_STATS_ACCURATE", "true");

        TableMetadata result = transformer.transform(source);

        assertFalse(result.getTableProperties().containsKey("numFiles"));
        assertFalse(result.getTableProperties().containsKey("numRows"));
        assertFalse(result.getTableProperties().containsKey("totalSize"));
        assertFalse(result.getTableProperties().containsKey("COLUMN_STATS_ACCURATE"));
    }

    @Test
    void testGenerateViewDdl() {
        TableMetadata view = TableMetadata.builder()
            .database("db1")
            .tableName("my_view")
            .viewOriginalText("SELECT * FROM table1 WHERE id > 100")
            .columns(List.of(
                HiveColumn.builder().name("id").type("int").build(),
                HiveColumn.builder().name("name").type("string").build()
            ))
            .build();

        String ddl = transformer.generateViewDdl(view);

        assertNotNull(ddl);
        assertTrue(ddl.contains("CREATE VIEW db1.my_view"));
        assertTrue(ddl.contains("SELECT * FROM table1 WHERE id > 100"));
        assertTrue(ddl.contains("(id, name)"));
    }

    @Test
    void testGenerateViewDdlWithoutColumns() {
        TableMetadata view = TableMetadata.builder()
            .database("db1")
            .tableName("simple_view")
            .viewOriginalText("SELECT * FROM table1")
            .columns(List.of())
            .build();

        String ddl = transformer.generateViewDdl(view);

        assertNotNull(ddl);
        assertTrue(ddl.contains("CREATE VIEW db1.simple_view"));
        assertFalse(ddl.contains("()"));
    }

    @Test
    void testGenerateViewDdlWithNullViewText() {
        TableMetadata view = TableMetadata.builder()
            .database("db1")
            .tableName("bad_view")
            .viewOriginalText(null)
            .build();

        String ddl = transformer.generateViewDdl(view);

        assertNull(ddl);
    }

    @Test
    void testHasUnsupportedFeaturesWithPartitionColumns() {
        TableMetadata source = TableMetadata.builder()
            .database("db1")
            .tableName("table1")
            .partitionColumns(Arrays.asList(
                HiveColumn.builder().name("part_col").type("UNIONTYPE<string>").build()
            ))
            .build();

        boolean hasUnsupported = transformer.hasUnsupportedFeatures(source);

        assertTrue(hasUnsupported);
    }

    @Test
    void testHasUnsupportedFeaturesNoIssues() {
        TableMetadata source = TableMetadata.builder()
            .database("db1")
            .tableName("table1")
            .columns(Arrays.asList(
                HiveColumn.builder().name("col1").type("string").build(),
                HiveColumn.builder().name("col2").type("int").build()
            ))
            .build();

        boolean hasUnsupported = transformer.hasUnsupportedFeatures(source);

        assertFalse(hasUnsupported);
    }

    @Test
    void testTransformDoesNotModifySource() {
        TableMetadata source = TableMetadata.builder()
            .database("db1")
            .tableName("table1")
            .location("hdfs://cdh-nn:8020/path")
            .tableType("MANAGED_TABLE")
            .tableProperties(new java.util.HashMap<>())
            .build();
        String originalLocation = source.getLocation();

        transformer.transform(source);

        // Source should not be modified
        assertEquals(originalLocation, source.getLocation());
    }

    @Test
    void testExternalTableDoesNotGetTransactionalProp() {
        TableMetadata source = TableMetadata.builder()
            .database("db1")
            .tableName("table1")
            .tableType("EXTERNAL_TABLE")
            .build();

        TableMetadata result = transformer.transform(source);

        // External tables should not get transactional=false
        assertNull(result.getTableProperties().get("transactional"));
    }

    @Test
    void testNullLocationDoesNotCauseError() {
        config.setRewriteLocations(true);
        TableMetadata source = TableMetadata.builder()
            .database("db1")
            .tableName("table1")
            .location(null)
            .build();

        // Should not throw
        TableMetadata result = transformer.transform(source);

        assertNull(result.getLocation());
    }
}