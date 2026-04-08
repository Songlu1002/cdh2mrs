package com.hadoop.migration.config;

import java.util.List;

public class MigrationTask {
    private String database;
    private List<String> tables;

    public String getDatabase() { return database; }
    public void setDatabase(String database) { this.database = database; }

    public List<String> getTables() { return tables; }
    public void setTables(List<String> tables) { this.tables = tables; }

    public boolean isMigrateAllTables() {
        return tables != null && tables.size() == 1 && "all".equalsIgnoreCase(tables.get(0));
    }
}
