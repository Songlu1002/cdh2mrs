package com.hadoop.migration.config;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class KerberosConfigTest {

    @Test
    void testDefaultValues() {
        KerberosConfig config = new KerberosConfig();
        assertFalse(config.isEnabled());
        assertNull(config.getPrincipal());
        assertNull(config.getKeytabPath());
        assertNull(config.getKrb5Conf());
    }

    @Test
    void testSettersAndGetters() {
        KerberosConfig config = new KerberosConfig();
        config.setEnabled(true);
        config.setPrincipal("hadoop@REALM");
        config.setKeytabPath("/path/to/keytab");
        config.setKrb5Conf("/path/to/krb5.conf");

        assertTrue(config.isEnabled());
        assertEquals("hadoop@REALM", config.getPrincipal());
        assertEquals("/path/to/keytab", config.getKeytabPath());
        assertEquals("/path/to/krb5.conf", config.getKrb5Conf());
    }
}
