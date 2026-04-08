package com.hadoop.migration.config;

public class ExecutionConfig {
    private boolean continueOnFailure = true;
    private int maxFailedTasks = 0;
    private int batchConcurrency = 1;

    // Getters and setters
    public boolean isContinueOnFailure() { return continueOnFailure; }
    public void setContinueOnFailure(boolean continueOnFailure) { this.continueOnFailure = continueOnFailure; }

    public int getMaxFailedTasks() { return maxFailedTasks; }
    public void setMaxFailedTasks(int maxFailedTasks) { this.maxFailedTasks = maxFailedTasks; }

    public int getBatchConcurrency() { return batchConcurrency; }
    public void setBatchConcurrency(int batchConcurrency) { this.batchConcurrency = batchConcurrency; }
}
