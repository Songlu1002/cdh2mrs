package com.hadoop.migration.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ClusterConfigTest {

    @Test
    void testSettersAndGetters() {
        ClusterConfig config = new ClusterConfig();
        config.setName("test-cluster");
        config.setType("CDH");
        config.setVersion("7.1.9");
        config.setHiveVersion("2.1.1");
        config.setConfigDir("/path/to/conf");

        assertEquals("test-cluster", config.getName());
        assertEquals("CDH", config.getType());
        assertEquals("7.1.9", config.getVersion());
        assertEquals("2.1.1", config.getHiveVersion());
        assertEquals("/path/to/conf", config.getConfigDir());
    }

    @Test
    void testHdfsConfig() {
        ClusterConfig config = new ClusterConfig();
        HdfsConfig hdfs = new HdfsConfig();
        hdfs.setNamenode("namenode.example.com");
        hdfs.setPort(9870);
        hdfs.setProtocol("webhdfs");

        config.setHdfs(hdfs);

        assertNotNull(config.getHdfs());
        assertEquals("namenode.example.com", config.getHdfs().getNamenode());
        assertEquals(9870, config.getHdfs().getPort());
        assertEquals("webhdfs", config.getHdfs().getProtocol());
    }

    @Test
    void testKerberosConfig() {
        ClusterConfig config = new ClusterConfig();
        KerberosConfig kerberos = new KerberosConfig();
        kerberos.setEnabled(true);
        kerberos.setPrincipal("hadoop@REALM");
        kerberos.setKeytabPath("/path/to/keytab");
        kerberos.setKrb5Conf("/path/to/krb5.conf");

        config.setKerberos(kerberos);

        assertNotNull(config.getKerberos());
        assertTrue(config.getKerberos().isEnabled());
        assertEquals("hadoop@REALM", config.getKerberos().getPrincipal());
        assertEquals("/path/to/keytab", config.getKerberos().getKeytabPath());
        assertEquals("/path/to/krb5.conf", config.getKerberos().getKrb5Conf());
    }

    @Test
    void testNullHdfs() {
        ClusterConfig config = new ClusterConfig();
        assertNull(config.getHdfs());
    }

    @Test
    void testNullKerberos() {
        ClusterConfig config = new ClusterConfig();
        assertNull(config.getKerberos());
    }
}
