package com.hadoop.migration.config;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MetadataConfigTest {

    @Test
    void testDefaultValues() {
        MetadataConfig config = new MetadataConfig();
        assertFalse(config.isMigrateViews());
        assertTrue(config.isWarnOnView());
        assertTrue(config.isRewriteLocations());
        assertEquals("SKIP", config.getUnsupportedAction());
    }

    @Test
    void testSetters() {
        MetadataConfig config = new MetadataConfig();
        config.setMigrateViews(true);
        config.setGenerateViewDdl(true);
        config.setUnsupportedAction("FAIL");

        assertTrue(config.isMigrateViews());
        assertTrue(config.isGenerateViewDdl());
        assertEquals("FAIL", config.getUnsupportedAction());
    }
}
