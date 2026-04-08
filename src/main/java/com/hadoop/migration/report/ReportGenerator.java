package com.hadoop.migration.report;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.hadoop.migration.model.MigrationResult;
import com.hadoop.migration.model.MigrationState;
import com.hadoop.migration.model.MigrationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReportGenerator {
    private static final Logger log = LoggerFactory.getLogger(ReportGenerator.class);

    private final String reportDir;
    private final ObjectMapper jsonMapper;
    private final DateTimeFormatter formatter;

    public ReportGenerator(String reportDir) {
        this.reportDir = reportDir;
        this.jsonMapper = new ObjectMapper();
        this.jsonMapper.enable(SerializationFeature.INDENT_OUTPUT);
        this.jsonMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        this.formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
            .withZone(ZoneId.systemDefault());
    }

    public String generateReport(MigrationState state, String sourceCluster, String targetCluster) {
        String timestamp = formatter.format(Instant.now());
        String baseName = "migration_report_" + timestamp;

        try {
            ensureReportDirExists();

            // Generate JSON report
            Map<String, Object> report = buildReport(state, sourceCluster, targetCluster, timestamp);
            String jsonPath = reportDir + "/" + baseName + ".json";
            jsonMapper.writeValue(new File(jsonPath), report);
            log.info("JSON report generated: {}", jsonPath);

            // Generate HTML report
            String htmlPath = reportDir + "/" + baseName + ".html";
            generateHtmlReport(report, htmlPath);
            log.info("HTML report generated: {}", htmlPath);

            return jsonPath;
        } catch (Exception e) {
            log.error("Failed to generate report", e);
            return null;
        }
    }

    private void ensureReportDirExists() throws IOException {
        Path dir = Paths.get(reportDir);
        if (!Files.exists(dir)) {
            Files.createDirectories(dir);
        }
    }

    private Map<String, Object> buildReport(MigrationState state, String source, String target, String timestamp) {
        Map<String, Object> report = new HashMap<>();

        // Summary
        Map<String, Object> summary = new HashMap<>();
        summary.put("generatedAt", timestamp);
        summary.put("sourceCluster", source);
        summary.put("targetCluster", target);
        summary.put("startTime", state.getStartTime());
        summary.put("endTime", state.getEndTime());
        summary.put("durationMs", state.getEndTime() - state.getStartTime());

        List<Map<String, Object>> tableDetails = new ArrayList<>();

        Map<String, MigrationResult> tableResults = state.getTableResults();
        long totalSize = 0;

        for (Map.Entry<String, MigrationResult> entry : tableResults.entrySet()) {
            String tableId = entry.getKey();
            MigrationResult result = entry.getValue();
            MigrationStatus status = result.getStatus();

            Map<String, Object> detail = new HashMap<>();
            String[] parts = tableId.split("\\.");
            detail.put("database", parts.length > 0 ? parts[0] : "unknown");
            detail.put("table", parts.length > 1 ? parts[1] : tableId);
            detail.put("status", status.name());
            detail.put("dataSizeBytes", result.getDataSizeBytes());
            detail.put("durationMs", result.getDurationMs());
            totalSize += result.getDataSizeBytes();

            if (result.getErrorCode() != null) {
                detail.put("errorCode", result.getErrorCode());
                detail.put("errorMessage", result.getErrorMessage());
            }

            tableDetails.add(detail);
        }

        summary.put("totalTables", state.getTotalCount());
        summary.put("successfulTables", state.getCompletedCount());
        summary.put("failedTables", state.getFailedCount());
        summary.put("pendingTables", 0);
        summary.put("totalDataSizeBytes", totalSize);
        summary.put("totalDataSizeFormatted", formatBytes(totalSize));

        report.put("migrationSummary", summary);
        report.put("tableDetails", tableDetails);

        // Compatibility issues summary
        Map<String, Object> compatibility = new HashMap<>();
        compatibility.put("autoResolved", state.getCompletedCount());
        compatibility.put("requiresAttention", state.getFailedCount());
        report.put("compatibilityIssues", compatibility);

        return report;
    }

    private void generateHtmlReport(Map<String, Object> report, String htmlPath) throws IOException {
        StringBuilder html = new StringBuilder();

        @SuppressWarnings("unchecked")
        Map<String, Object> summary = (Map<String, Object>) report.get("migrationSummary");

        html.append("<!DOCTYPE html>\n");
        html.append("<html><head>\n");
        html.append("<meta charset=\"UTF-8\">\n");
        html.append("<title>Migration Report</title>\n");
        html.append("<style>\n");
        html.append("body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }\n");
        html.append("h1 { color: #333; }\n");
        html.append(".summary { background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }\n");
        html.append(".metric { display: inline-block; margin: 10px 20px 10px 0; }\n");
        html.append(".metric-label { font-size: 12px; color: #666; }\n");
        html.append(".metric-value { font-size: 24px; font-weight: bold; }\n");
        html.append(".success { color: #28a745; }\n");
        html.append(".failed { color: #dc3545; }\n");
        html.append(".pending { color: #ffc107; }\n");
        html.append("table { width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; }\n");
        html.append("th { background: #007bff; color: white; padding: 12px; text-align: left; }\n");
        html.append("td { padding: 10px 12px; border-bottom: 1px solid #eee; }\n");
        html.append("tr:hover { background: #f8f9fa; }\n");
        html.append(".status { padding: 4px 8px; border-radius: 4px; font-size: 12px; }\n");
        html.append(".status-COMPLETED { background: #d4edda; color: #155724; }\n");
        html.append(".status-FAILED { background: #f8d7da; color: #721c24; }\n");
        html.append(".status-PENDING { background: #fff3cd; color: #856404; }\n");
        html.append(".status-DATA_COPYING { background: #cce5ff; color: #004085; }\n");
        html.append(".status-METADATA_MIGRATING { background: #e2e3e5; color: #383d41; }\n");
        html.append(".error { font-size: 12px; color: #dc3545; }\n");
        html.append("</style>\n");
        html.append("</head><body>\n");

        html.append("<h1>Hadoop Migration Report</h1>\n");

        // Summary section
        html.append("<div class=\"summary\">\n");
        html.append("<h2>Summary</h2>\n");
        html.append("<div class=\"metric\">\n");
        html.append("<div class=\"metric-label\">Source</div>\n");
        html.append("<div class=\"metric-value\">").append(escapeHtml((String) summary.get("sourceCluster"))).append("</div>\n");
        html.append("</div>\n");
        html.append("<div class=\"metric\">\n");
        html.append("<div class=\"metric-label\">Target</div>\n");
        html.append("<div class=\"metric-value\">").append(escapeHtml((String) summary.get("targetCluster"))).append("</div>\n");
        html.append("</div>\n");
        html.append("<div class=\"metric\">\n");
        html.append("<div class=\"metric-label\">Total Tables</div>\n");
        html.append("<div class=\"metric-value\">").append(summary.get("totalTables")).append("</div>\n");
        html.append("</div>\n");
        html.append("<div class=\"metric\">\n");
        html.append("<div class=\"metric-label\">Successful</div>\n");
        html.append("<div class=\"metric-value success\">").append(summary.get("successfulTables")).append("</div>\n");
        html.append("</div>\n");
        html.append("<div class=\"metric\">\n");
        html.append("<div class=\"metric-label\">Failed</div>\n");
        html.append("<div class=\"metric-value failed\">").append(summary.get("failedTables")).append("</div>\n");
        html.append("</div>\n");
        html.append("<div class=\"metric\">\n");
        html.append("<div class=\"metric-label\">Total Data</div>\n");
        html.append("<div class=\"metric-value\">").append(summary.get("totalDataSizeFormatted")).append("</div>\n");
        html.append("</div>\n");
        html.append("<div class=\"metric\">\n");
        html.append("<div class=\"metric-label\">Duration</div>\n");
        html.append("<div class=\"metric-value\">").append(formatDuration((Long) summary.get("durationMs"))).append("</div>\n");
        html.append("</div>\n");
        html.append("</div>\n");

        // Table details
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> tables = (List<Map<String, Object>>) report.get("tableDetails");
        if (!tables.isEmpty()) {
            html.append("<h2>Table Details</h2>\n");
            html.append("<table><thead><tr>\n");
            html.append("<th>Database</th>\n");
            html.append("<th>Table</th>\n");
            html.append("<th>Status</th>\n");
            html.append("<th>Data Size</th>\n");
            html.append("<th>Duration</th>\n");
            html.append("<th>Notes</th>\n");
            html.append("</tr></thead><tbody>\n");

            for (Map<String, Object> table : tables) {
                html.append("<tr>\n");
                html.append("<td>").append(escapeHtml((String) table.get("database"))).append("</td>\n");
                html.append("<td>").append(escapeHtml((String) table.get("table"))).append("</td>\n");
                String status = (String) table.get("status");
                html.append("<td><span class=\"status status-").append(status).append("\">").append(status).append("</span></td>\n");
                html.append("<td>").append(formatBytes(((Number) table.getOrDefault("dataSizeBytes", 0)).longValue())).append("</td>\n");
                html.append("<td>").append(formatDuration(((Number) table.getOrDefault("durationMs", 0)).longValue())).append("</td>\n");

                StringBuilder notes = new StringBuilder();
                if (table.get("errorCode") != null) {
                    notes.append("<span class=\"error\">").append(escapeHtml((String) table.get("errorCode")));
                    if (table.get("errorMessage") != null) {
                        notes.append(": ").append(escapeHtml((String) table.get("errorMessage")));
                    }
                    notes.append("</span>");
                }
                html.append("<td>").append(notes.toString()).append("</td>\n");
                html.append("</tr>\n");
            }

            html.append("</tbody></table>\n");
        }

        html.append("</body></html>\n");

        Files.write(Paths.get(htmlPath), html.toString().getBytes());
    }

    private String escapeHtml(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                   .replace("<", "&lt;")
                   .replace(">", "&gt;")
                   .replace("\"", "&quot;");
    }

    private String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        int unit = 0;
        double value = bytes;
        String[] units = {"B", "KB", "MB", "GB", "TB", "PB"};
        while (value >= 1024 && unit < units.length - 1) {
            value /= 1024;
            unit++;
        }
        return String.format("%.2f %s", value, units[unit]);
    }

    private String formatDuration(long ms) {
        if (ms < 1000) return ms + "ms";
        long seconds = ms / 1000;
        if (seconds < 60) return seconds + "s";
        long minutes = seconds / 60;
        seconds = seconds % 60;
        if (minutes < 60) return String.format("%dm %ds", minutes, seconds);
        long hours = minutes / 60;
        minutes = minutes % 60;
        return String.format("%dh %dm %ds", hours, minutes, seconds);
    }
}
