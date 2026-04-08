package com.hadoop.migration.model;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class HiveColumnTest {

    @Test
    void testBuilder() {
        HiveColumn column = HiveColumn.builder()
            .name("id")
            .type("bigint")
            .comment("Primary key")
            .build();

        assertEquals("id", column.getName());
        assertEquals("bigint", column.getType());
        assertEquals("Primary key", column.getComment());
    }

    @Test
    void testIsPartition() {
        HiveColumn regular = HiveColumn.builder().name("col").type("string").build();
        HiveColumn partition = HiveColumn.builder().name("dt").type("string").isPartition(true).build();

        assertFalse(regular.isPartition());
        assertTrue(partition.isPartition());
    }
}