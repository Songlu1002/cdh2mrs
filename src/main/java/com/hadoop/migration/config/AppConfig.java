package com.hadoop.migration.config;

import java.util.List;

public class AppConfig {
    private Clusters clusters;
    private MigrationConfig migration;

    // Getters and setters
    public Clusters getClusters() { return clusters; }
    public void setClusters(Clusters clusters) { this.clusters = clusters; }

    public MigrationConfig getMigration() { return migration; }
    public void setMigration(MigrationConfig migration) { this.migration = migration; }

    public static class Clusters {
        private ClusterConfig source;
        private ClusterConfig target;

        public ClusterConfig getSource() { return source; }
        public void setSource(ClusterConfig source) { this.source = source; }

        public ClusterConfig getTarget() { return target; }
        public void setTarget(ClusterConfig target) { this.target = target; }
    }

    public static class MigrationConfig {
        private List<MigrationTask> tasks;
        private DistcpConfig distcp;
        private ExecutionConfig execution;
        private OutputConfig output;

        public List<MigrationTask> getTasks() { return tasks; }
        public void setTasks(List<MigrationTask> tasks) { this.tasks = tasks; }

        public DistcpConfig getDistcp() { return distcp; }
        public void setDistcp(DistcpConfig distcp) { this.distcp = distcp; }

        public ExecutionConfig getExecution() { return execution; }
        public void setExecution(ExecutionConfig execution) { this.execution = execution; }

        public OutputConfig getOutput() { return output; }
        public void setOutput(OutputConfig output) { this.output = output; }
    }

    public static class OutputConfig {
        private String stateFile = "state/migration-state.json";
        private String reportDir = "reports/";

        public String getStateFile() { return stateFile; }
        public void setStateFile(String stateFile) { this.stateFile = stateFile; }

        public String getReportDir() { return reportDir; }
        public void setReportDir(String reportDir) { this.reportDir = reportDir; }
    }
}
