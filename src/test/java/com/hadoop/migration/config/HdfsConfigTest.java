package com.hadoop.migration.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HdfsConfigTest {

    @Test
    void testDefaultValues() {
        HdfsConfig config = new HdfsConfig();
        assertEquals(9870, config.getPort());
        assertEquals("webhdfs", config.getProtocol());
        assertNull(config.getNamenode());
    }

    @Test
    void testSetters() {
        HdfsConfig config = new HdfsConfig();
        config.setNamenode("cdh-nn.example.com");
        config.setPort(9000);
        config.setProtocol("hdfs");

        assertEquals("cdh-nn.example.com", config.getNamenode());
        assertEquals(9000, config.getPort());
        assertEquals("hdfs", config.getProtocol());
    }

    @Test
    void testGetFullPath() {
        HdfsConfig config = new HdfsConfig();
        config.setNamenode("cdh-nn");
        config.setPort(9870);
        config.setProtocol("webhdfs");

        assertEquals("webhdfs://cdh-nn:9870/path/to/table", config.getFullPath("/path/to/table"));
        assertEquals("webhdfs://cdh-nn:9870/", config.getFullPath("/"));
    }

    @Test
    void testGetFullPathWithDifferentProtocol() {
        HdfsConfig config = new HdfsConfig();
        config.setNamenode("hdfs-nn");
        config.setPort(8020);
        config.setProtocol("hdfs");

        assertEquals("hdfs://hdfs-nn:8020/db/table", config.getFullPath("/db/table"));
    }
}
