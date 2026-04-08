package com.hadoop.migration.config;

public class ClusterConfig {
    private String name;
    private String type;
    private String version;
    private String hiveVersion;
    private String configDir;
    private HdfsConfig hdfs;
    private KerberosConfig kerberos;

    // Getters and setters
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getVersion() { return version; }
    public void setVersion(String version) { this.version = version; }

    public String getHiveVersion() { return hiveVersion; }
    public void setHiveVersion(String hiveVersion) { this.hiveVersion = hiveVersion; }

    public String getConfigDir() { return configDir; }
    public void setConfigDir(String configDir) { this.configDir = configDir; }

    public HdfsConfig getHdfs() { return hdfs; }
    public void setHdfs(HdfsConfig hdfs) { this.hdfs = hdfs; }

    public KerberosConfig getKerberos() { return kerberos; }
    public void setKerberos(KerberosConfig kerberos) { this.kerberos = kerberos; }
}
