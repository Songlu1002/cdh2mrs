package com.hadoop.migration.config;

public class HdfsConfig {
    private String namenode;
    private int port = 9870;
    private String protocol = "webhdfs";

    public String getNamenode() { return namenode; }
    public void setNamenode(String namenode) { this.namenode = namenode; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public String getProtocol() { return protocol; }
    public void setProtocol(String protocol) { this.protocol = protocol; }

    public String getFullPath(String path) {
        return protocol + "://" + namenode + ":" + port + path;
    }
}
