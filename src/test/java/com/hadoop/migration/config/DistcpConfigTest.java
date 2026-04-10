package com.hadoop.migration.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DistcpConfigTest {

    @Test
    void testDefaultValues() {
        DistcpConfig config = new DistcpConfig();
        assertEquals(20, config.getMapTasks());
        assertEquals(100, config.getBandwidthMB());
        assertEquals(3, config.getRetryCount());
        assertEquals("webhdfs", config.getSourceProtocol());
        assertEquals("webhdfs", config.getTargetProtocol());
        assertEquals("/warehouse/tablespace/external/hive/", config.getExternalTablePath());
    }

    @Test
    void testSetters() {
        DistcpConfig config = new DistcpConfig();
        config.setMapTasks(50);
        config.setBandwidthMB(200);
        config.setRetryCount(5);
        config.setSourceProtocol("hftp");
        config.setTargetProtocol("webhdfs");

        assertEquals(50, config.getMapTasks());
        assertEquals(200, config.getBandwidthMB());
        assertEquals(5, config.getRetryCount());
        assertEquals("hftp", config.getSourceProtocol());
        assertEquals("webhdfs", config.getTargetProtocol());
    }

    @Test
    void testExternalTablePath() {
        DistcpConfig config = new DistcpConfig();

        // Default value
        assertEquals("/warehouse/tablespace/external/hive/", config.getExternalTablePath());

        // Custom path
        config.setExternalTablePath("/custom/path/");
        assertEquals("/custom/path/", config.getExternalTablePath());
    }

    @Test
    void testMapTasksBoundaries() {
        DistcpConfig config = new DistcpConfig();

        config.setMapTasks(1);
        assertEquals(1, config.getMapTasks());

        config.setMapTasks(1000);
        assertEquals(1000, config.getMapTasks());
    }

    @Test
    void testBandwidthMBBoundaries() {
        DistcpConfig config = new DistcpConfig();

        config.setBandwidthMB(1);
        assertEquals(1, config.getBandwidthMB());

        config.setBandwidthMB(10000);
        assertEquals(10000, config.getBandwidthMB());
    }

    @Test
    void testRetryCountBoundaries() {
        DistcpConfig config = new DistcpConfig();

        config.setRetryCount(0);
        assertEquals(0, config.getRetryCount());

        config.setRetryCount(10);
        assertEquals(10, config.getRetryCount());
    }
}
