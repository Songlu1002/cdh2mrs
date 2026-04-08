package com.hadoop.migration.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AppConfigTest {

    @Test
    void testParseMinimalConfig() throws Exception {
        String yaml = """
            clusters:
              source:
                name: "test-source"
                type: "CDH"
                version: "7.1.9"
                hiveVersion: "2.1.1"
                hdfs:
                  namenode: "cdh-nn"
                  port: 9870
              target:
                name: "test-target"
                type: "MRS"
                version: "3.5.0"
                hiveVersion: "3.1.0"
                hdfs:
                  namenode: "mrs-nn"
                  port: 9870
            migration:
              tasks:
                - database: "db1"
                  tables: ["table1"]
              distcp:
                mapTasks: 10
              execution:
                continueOnFailure: true
            """;

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        AppConfig config = mapper.readValue(yaml, AppConfig.class);

        assertEquals("test-source", config.getClusters().getSource().getName());
        assertEquals("cdh-nn", config.getClusters().getSource().getHdfs().getNamenode());
        assertEquals(1, config.getMigration().getTasks().size());
        assertEquals("db1", config.getMigration().getTasks().get(0).getDatabase());
    }

    @Test
    void testDefaultValues() throws Exception {
        String yaml = """
            clusters:
              source:
                name: "source"
                hdfs:
                  namenode: "nn"
              target:
                name: "target"
                hdfs:
                  namenode: "nn"
            migration:
              tasks: []
              distcp: {}
              execution: {}
            """;

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        AppConfig config = mapper.readValue(yaml, AppConfig.class);

        assertEquals(20, config.getMigration().getDistcp().getMapTasks());
        assertEquals(100, config.getMigration().getDistcp().getBandwidthMB());
        assertTrue(config.getMigration().getExecution().isContinueOnFailure());
    }
}
