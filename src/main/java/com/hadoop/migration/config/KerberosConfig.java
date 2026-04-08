package com.hadoop.migration.config;

public class KerberosConfig {
    private boolean enabled = false;
    private String principal;
    private String keytabPath;
    private String krb5Conf;

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public String getPrincipal() { return principal; }
    public void setPrincipal(String principal) { this.principal = principal; }

    public String getKeytabPath() { return keytabPath; }
    public void setKeytabPath(String keytabPath) { this.keytabPath = keytabPath; }

    public String getKrb5Conf() { return krb5Conf; }
    public void setKrb5Conf(String krb5Conf) { this.krb5Conf = krb5Conf; }
}
