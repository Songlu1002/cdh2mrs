package com.hadoop.migration.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableMetadata {
    private String database;
    private String tableName;
    private String tableType;
    private String location;
    private String inputFormat;
    private String outputFormat;
    private String serdeClass;
    private List<HiveColumn> columns;
    private List<HiveColumn> partitionColumns;
    private Map<String, String> tableProperties;
    private String viewOriginalText;
    private boolean isView;

    public TableMetadata() {
        this.columns = new ArrayList<>();
        this.partitionColumns = new ArrayList<>();
        this.tableProperties = new HashMap<>();
    }

    private TableMetadata(Builder builder) {
        this.database = builder.database;
        this.tableName = builder.tableName;
        this.tableType = builder.tableType;
        this.location = builder.location;
        this.inputFormat = builder.inputFormat;
        this.outputFormat = builder.outputFormat;
        this.serdeClass = builder.serdeClass;
        this.columns = builder.columns;
        this.partitionColumns = builder.partitionColumns;
        this.tableProperties = builder.tableProperties;
        this.viewOriginalText = builder.viewOriginalText;
        this.isView = builder.isView;
    }

    public static Builder builder() { return new Builder(); }

    public String getDatabase() { return database; }
    public void setDatabase(String database) { this.database = database; }

    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }

    public String getTableType() { return tableType; }
    public void setTableType(String tableType) { this.tableType = tableType; }

    public String getLocation() { return location; }
    public void setLocation(String location) { this.location = location; }

    public String getInputFormat() { return inputFormat; }
    public void setInputFormat(String inputFormat) { this.inputFormat = inputFormat; }

    public String getOutputFormat() { return outputFormat; }
    public void setOutputFormat(String outputFormat) { this.outputFormat = outputFormat; }

    public String getSerdeClass() { return serdeClass; }
    public void setSerdeClass(String serdeClass) { this.serdeClass = serdeClass; }

    public List<HiveColumn> getColumns() { return columns; }
    public void setColumns(List<HiveColumn> columns) { this.columns = columns; }

    public List<HiveColumn> getPartitionColumns() { return partitionColumns; }
    public void setPartitionColumns(List<HiveColumn> partitionColumns) { this.partitionColumns = partitionColumns; }

    public Map<String, String> getTableProperties() { return tableProperties; }
    public void setTableProperties(Map<String, String> tableProperties) { this.tableProperties = tableProperties; }

    public String getViewOriginalText() { return viewOriginalText; }
    public void setViewOriginalText(String viewOriginalText) { this.viewOriginalText = viewOriginalText; }

    public boolean isView() { return isView; }
    public void setView(boolean view) { isView = view; }

    public boolean isExternal() {
        return "EXTERNAL_TABLE".equalsIgnoreCase(tableType);
    }

    public static class Builder {
        private String database;
        private String tableName;
        private String tableType;
        private String location;
        private String inputFormat;
        private String outputFormat;
        private String serdeClass;
        private List<HiveColumn> columns = new ArrayList<>();
        private List<HiveColumn> partitionColumns = new ArrayList<>();
        private Map<String, String> tableProperties = new HashMap<>();
        private String viewOriginalText;
        private boolean isView;

        public Builder database(String database) { this.database = database; return this; }
        public Builder tableName(String tableName) { this.tableName = tableName; return this; }
        public Builder tableType(String tableType) { this.tableType = tableType; return this; }
        public Builder location(String location) { this.location = location; return this; }
        public Builder inputFormat(String inputFormat) { this.inputFormat = inputFormat; return this; }
        public Builder outputFormat(String outputFormat) { this.outputFormat = outputFormat; return this; }
        public Builder serdeClass(String serdeClass) { this.serdeClass = serdeClass; return this; }
        public Builder columns(List<HiveColumn> columns) { this.columns = columns; return this; }
        public Builder partitionColumns(List<HiveColumn> partitionColumns) { this.partitionColumns = partitionColumns; return this; }
        public Builder tableProperties(Map<String, String> tableProperties) { this.tableProperties = tableProperties; return this; }
        public Builder viewOriginalText(String viewOriginalText) { this.viewOriginalText = viewOriginalText; return this; }
        public Builder isView(boolean isView) { this.isView = isView; return this; }

        public TableMetadata build() { return new TableMetadata(this); }
    }
}