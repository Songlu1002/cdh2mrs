package com.hadoop.migration.auth;

import com.hadoop.migration.config.KerberosConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class KerberosAuthenticator {
    private static final Logger log = LoggerFactory.getLogger(KerberosAuthenticator.class);

    public static void authenticate(KerberosConfig config) {
        if (config == null || !config.isEnabled()) {
            log.info("Kerberos authentication is disabled");
            return;
        }

        validateConfig(config);

        try {
            Configuration hadoopConf = new Configuration();

            // Set krb5.conf if specified
            if (config.getKrb5Conf() != null && !config.getKrb5Conf().isEmpty()) {
                File krb5Conf = new File(config.getKrb5Conf());
                if (krb5Conf.exists()) {
                    System.setProperty("java.security.krb5.conf", krb5Conf.getAbsolutePath());
                    log.info("Set Kerberos configuration: {}", krb5Conf.getAbsolutePath());
                } else {
                    log.warn("Kerberos config file not found: {}", config.getKrb5Conf());
                }
            }

            // Enable Kerberos authentication
            hadoopConf.set("hadoop.security.authentication", "kerberos");

            UserGroupInformation.setConfiguration(hadoopConf);
            UserGroupInformation.loginUserFromKeytab(
                config.getPrincipal(),
                config.getKeytabPath()
            );

            UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
            log.info("Successfully authenticated as: {}", currentUser.getUserName());

        } catch (Exception e) {
            log.error("Kerberos authentication failed", e);
            throw new RuntimeException("Kerberos authentication failed: " + e.getMessage(), e);
        }
    }

    private static void validateConfig(KerberosConfig config) {
        if (config.getPrincipal() == null || config.getPrincipal().isEmpty()) {
            throw new IllegalArgumentException("Kerberos principal is required when Kerberos is enabled");
        }
        if (config.getKeytabPath() == null || config.getKeytabPath().isEmpty()) {
            throw new IllegalArgumentException("Kerberos keytab path is required when Kerberos is enabled");
        }
        File keytab = new File(config.getKeytabPath());
        if (!keytab.exists()) {
            throw new IllegalArgumentException("Kerberos keytab file does not exist: " + keytab.getAbsolutePath());
        }
    }

    public static boolean isAuthenticated() {
        try {
            UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
            return ugi != null && ugi.hasKerberosCredentials();
        } catch (IOException e) {
            log.warn("Failed to check Kerberos authentication status", e);
            return false;
        }
    }
}