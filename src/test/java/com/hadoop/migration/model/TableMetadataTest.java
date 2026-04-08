package com.hadoop.migration.model;

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import static org.junit.jupiter.api.Assertions.*;

class TableMetadataTest {

    @Test
    void testBuilder() {
        TableMetadata table = TableMetadata.builder()
            .database("sales_db")
            .tableName("orders")
            .tableType("MANAGED_TABLE")
            .location("hdfs://nn:8020/warehouse/sales_db.db/orders")
            .inputFormat("org.apache.hadoop.mapred.TextInputFormat")
            .outputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
            .serdeClass("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
            .columns(Arrays.asList(
                HiveColumn.builder().name("id").type("bigint").build(),
                HiveColumn.builder().name("name").type("string").build()
            ))
            .build();

        assertEquals("sales_db", table.getDatabase());
        assertEquals("orders", table.getTableName());
        assertEquals("MANAGED_TABLE", table.getTableType());
        assertEquals(2, table.getColumns().size());
    }

    @Test
    void testIsExternal() {
        TableMetadata managed = TableMetadata.builder().tableName("t").tableType("MANAGED_TABLE").build();
        TableMetadata external = TableMetadata.builder().tableName("t").tableType("EXTERNAL_TABLE").build();

        assertFalse(managed.isExternal());
        assertTrue(external.isExternal());
    }
}