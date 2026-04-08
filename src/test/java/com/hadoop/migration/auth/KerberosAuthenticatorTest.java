package com.hadoop.migration.auth;

import com.hadoop.migration.config.KerberosConfig;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class KerberosAuthenticatorTest {

    @Test
    void testDisabledKerberosDoesNotThrow() {
        KerberosConfig config = new KerberosConfig();
        config.setEnabled(false);

        // Should not throw when Kerberos is disabled
        assertDoesNotThrow(() -> KerberosAuthenticator.authenticate(config));
    }

    @Test
    void testEnabledKerberosWithMissingPrincipalThrows() {
        KerberosConfig config = new KerberosConfig();
        config.setEnabled(true);
        config.setKeytabPath("/path/to/keytab");
        // principal is null

        assertThrows(IllegalArgumentException.class,
            () -> KerberosAuthenticator.authenticate(config));
    }

    @Test
    void testEnabledKerberosWithMissingKeytabThrows() {
        KerberosConfig config = new KerberosConfig();
        config.setEnabled(true);
        config.setPrincipal("hadoop@REALM");
        // keytabPath is null

        assertThrows(IllegalArgumentException.class,
            () -> KerberosAuthenticator.authenticate(config));
    }
}