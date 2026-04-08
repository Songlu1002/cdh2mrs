package com.hadoop.migration.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AppConfigTest {

    @Test
    void testParseMinimalConfig() throws Exception {
        String yaml =
            "clusters:\n" +
            "  source:\n" +
            "    name: \"test-source\"\n" +
            "    type: \"CDH\"\n" +
            "    version: \"7.1.9\"\n" +
            "    hiveVersion: \"2.1.1\"\n" +
            "    hdfs:\n" +
            "      namenode: \"cdh-nn\"\n" +
            "      port: 9870\n" +
            "  target:\n" +
            "    name: \"test-target\"\n" +
            "    type: \"MRS\"\n" +
            "    version: \"3.5.0\"\n" +
            "    hiveVersion: \"3.1.0\"\n" +
            "    hdfs:\n" +
            "      namenode: \"mrs-nn\"\n" +
            "      port: 9870\n" +
            "migration:\n" +
            "  tasks:\n" +
            "    - database: \"db1\"\n" +
            "      tables: [\"table1\"]\n" +
            "  distcp:\n" +
            "    mapTasks: 10\n" +
            "  execution:\n" +
            "    continueOnFailure: true\n";

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        AppConfig config = mapper.readValue(yaml, AppConfig.class);

        assertEquals("test-source", config.getClusters().getSource().getName());
        assertEquals("cdh-nn", config.getClusters().getSource().getHdfs().getNamenode());
        assertEquals(1, config.getMigration().getTasks().size());
        assertEquals("db1", config.getMigration().getTasks().get(0).getDatabase());
    }

    @Test
    void testDefaultValues() throws Exception {
        String yaml =
            "clusters:\n" +
            "  source:\n" +
            "    name: \"source\"\n" +
            "    hdfs:\n" +
            "      namenode: \"nn\"\n" +
            "  target:\n" +
            "    name: \"target\"\n" +
            "    hdfs:\n" +
            "      namenode: \"nn\"\n" +
            "migration:\n" +
            "  tasks: []\n" +
            "  distcp: {}\n" +
            "  execution: {}\n"
;

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        AppConfig config = mapper.readValue(yaml, AppConfig.class);

        assertEquals(20, config.getMigration().getDistcp().getMapTasks());
        assertEquals(100, config.getMigration().getDistcp().getBandwidthMB());
        assertTrue(config.getMigration().getExecution().isContinueOnFailure());
    }
}
