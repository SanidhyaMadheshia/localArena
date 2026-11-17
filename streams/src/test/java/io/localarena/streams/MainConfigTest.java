package io.localarena.streams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.junit.jupiter.api.Test;

class MainConfigTest {
    private static java.util.Properties cfg(Map<String, String> env) {
        return Main.config(env::get);
    }

    @Test
    void plaintextSetsNoSecurity() {
        var p = cfg(Map.of());
        assertFalse(p.containsKey("security.protocol"));
        assertFalse(p.containsKey("sasl.jaas.config"));
        assertEquals("PLAINTEXT", cfg(Map.of("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")).getProperty("security.protocol", "PLAINTEXT"));
    }

    @Test
    void saslSslMapsEnv() {
        var p = cfg(Map.of("KAFKA_SECURITY_PROTOCOL", "SASL_SSL", "KAFKA_USERNAME", "streams",
                "KAFKA_PASSWORD", "pa\"ss\\w", "KAFKA_CA_FILE", "/etc/kafka-ca/ca.crt"));
        assertEquals("SASL_SSL", p.get("security.protocol"));
        assertEquals("SCRAM-SHA-512", p.get("sasl.mechanism"));
        assertEquals("PEM", p.get("ssl.truststore.type"));
        assertEquals("/etc/kafka-ca/ca.crt", p.get("ssl.truststore.location"));
        assertEquals("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"streams\" "
                + "password=\"pa\\\"ss\\\\w\";", p.get("sasl.jaas.config"));
    }

    @Test
    void validation() {
        assertThrows(IllegalArgumentException.class, () -> cfg(Map.of("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")));
        assertThrows(IllegalArgumentException.class, () -> cfg(Map.of("KAFKA_SECURITY_PROTOCOL", "BOGUS")));
    }
}
