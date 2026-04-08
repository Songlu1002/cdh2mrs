package com.hadoop.migration.config;

public class DistcpConfig {
    private int mapTasks = 20;
    private int bandwidthMB = 100;
    private int retryCount = 3;
    private String sourceProtocol = "webhdfs";
    private String targetProtocol = "webhdfs";

    // Getters and setters
    public int getMapTasks() { return mapTasks; }
    public void setMapTasks(int mapTasks) { this.mapTasks = mapTasks; }

    public int getBandwidthMB() { return bandwidthMB; }
    public void setBandwidthMB(int bandwidthMB) { this.bandwidthMB = bandwidthMB; }

    public int getRetryCount() { return retryCount; }
    public void setRetryCount(int retryCount) { this.retryCount = retryCount; }

    public String getSourceProtocol() { return sourceProtocol; }
    public void setSourceProtocol(String sourceProtocol) { this.sourceProtocol = sourceProtocol; }

    public String getTargetProtocol() { return targetProtocol; }
    public void setTargetProtocol(String targetProtocol) { this.targetProtocol = targetProtocol; }
}
