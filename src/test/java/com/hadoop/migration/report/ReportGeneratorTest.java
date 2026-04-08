package com.hadoop.migration.report;

import com.hadoop.migration.model.MigrationResult;
import com.hadoop.migration.model.MigrationState;
import com.hadoop.migration.model.MigrationStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ReportGeneratorTest {

    @TempDir
    Path tempDir;

    private MigrationState state;

    @BeforeEach
    void setUp() {
        state = new MigrationState();
        state.setSourceCluster("cdh-cluster");
        state.setTargetCluster("mrs-cluster");
        state.setStartTime(System.currentTimeMillis() - 60000);
        state.setEndTime(System.currentTimeMillis());

        // Add successful result
        MigrationResult success = MigrationResult.builder()
            .database("sales_db")
            .table("orders")
            .status(MigrationStatus.COMPLETED)
            .dataSize(1024 * 1024 * 100)
            .build();
        state.putResult("sales_db", "orders", success);

        // Add failed result
        MigrationResult failed = MigrationResult.builder()
            .database("sales_db")
            .table("failed_table")
            .status(MigrationStatus.FAILED)
            .error("DISTCP_1", "Copy failed")
            .build();
        state.putResult("sales_db", "failed_table", failed);
    }

    @Test
    void generateReport_createsJsonAndHtml() {
        String reportDir = tempDir.toString();
        ReportGenerator generator = new ReportGenerator(reportDir);

        String jsonPath = generator.generateReport(state, "cdh", "mrs");

        assertNotNull(jsonPath);
        assertTrue(jsonPath.endsWith(".json"));
        assertTrue(Files.exists(Path.of(jsonPath)));

        String htmlPath = jsonPath.replace(".json", ".html");
        assertTrue(Files.exists(Path.of(htmlPath)));
    }

    @Test
    void generateReport_containsCorrectSummary() throws Exception {
        String reportDir = tempDir.toString();
        ReportGenerator generator = new ReportGenerator(reportDir);

        String jsonPath = generator.generateReport(state, "cdh", "mrs");

        String content = Files.readString(Path.of(jsonPath));

        assertTrue(content.contains("\"sourceCluster\" : \"cdh\""));
        assertTrue(content.contains("\"targetCluster\" : \"mrs\""));
        assertTrue(content.contains("\"totalTables\" : 2"));
        assertTrue(content.contains("\"successfulTables\" : 1"));
        assertTrue(content.contains("\"failedTables\" : 1"));
    }

    @Test
    void generateReport_containsTableDetails() throws Exception {
        String reportDir = tempDir.toString();
        ReportGenerator generator = new ReportGenerator(reportDir);

        String jsonPath = generator.generateReport(state, "cdh", "mrs");

        String content = Files.readString(Path.of(jsonPath));

        assertTrue(content.contains("sales_db"));
        assertTrue(content.contains("orders"));
        assertTrue(content.contains("failed_table"));
    }

    @Test
    void generateReport_htmlIsValid() throws Exception {
        String reportDir = tempDir.toString();
        ReportGenerator generator = new ReportGenerator(reportDir);

        String jsonPath = generator.generateReport(state, "cdh", "mrs");
        String htmlPath = jsonPath.replace(".json", ".html");

        String html = Files.readString(Path.of(htmlPath));

        assertTrue(html.contains("<!DOCTYPE html>"));
        assertTrue(html.contains("<table>"));
        assertTrue(html.contains("Migration Report"));
    }
}
