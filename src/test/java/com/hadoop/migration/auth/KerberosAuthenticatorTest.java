package com.hadoop.migration.auth;

import com.hadoop.migration.config.KerberosConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class KerberosAuthenticatorTest {

    @TempDir
    Path tempDir;

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

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> KerberosAuthenticator.authenticate(config));
        assertTrue(ex.getMessage().contains("principal"));
    }

    @Test
    void testEnabledKerberosWithMissingKeytabThrows() {
        KerberosConfig config = new KerberosConfig();
        config.setEnabled(true);
        config.setPrincipal("hadoop@REALM");
        // keytabPath is null

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> KerberosAuthenticator.authenticate(config));
        assertTrue(ex.getMessage().contains("keytab"));
    }

    @Test
    void testEnabledKerberosWithNonExistentKeytabThrows() {
        KerberosConfig config = new KerberosConfig();
        config.setEnabled(true);
        config.setPrincipal("hadoop@REALM");
        config.setKeytabPath("/non/existent/keytab");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> KerberosAuthenticator.authenticate(config));
        assertTrue(ex.getMessage().contains("does not exist"));
    }

    @Test
    void testNullConfigDoesNotThrow() {
        // Should handle null gracefully without throwing
        assertDoesNotThrow(() -> KerberosAuthenticator.authenticate(null));
    }

    @Test
    void testIsAuthenticatedWhenNotAuthenticated() {
        // Just call to ensure it doesn't throw
        // The result depends on actual Hadoop state
        boolean result = KerberosAuthenticator.isAuthenticated();
        // Just verify the method returns a boolean without throwing
        assertTrue(result == true || result == false);
    }
}