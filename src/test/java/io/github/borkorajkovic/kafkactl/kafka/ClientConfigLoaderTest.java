package io.github.borkorajkovic.kafkactl.kafka;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClientConfigLoaderTest {

    @Test
    void bootstrapServersOnlyWhenNoConfigFileGiven() throws IOException {
        Properties props = ClientConfigLoader.load("localhost:9092", null);
        assertEquals("localhost:9092", props.getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(1, props.size());
    }

    @Test
    void mergesPropertiesFromConfigFile(@TempDir Path tempDir) throws IOException {
        Path configFile = tempDir.resolve("client.properties");
        Files.writeString(configFile, """
                security.protocol=SASL_SSL
                sasl.mechanism=PLAIN
                sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="user" password="pass";
                """);

        Properties props = ClientConfigLoader.load("broker:9093", configFile);

        assertEquals("broker:9093", props.getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("SASL_SSL", props.getProperty("security.protocol"));
        assertEquals("PLAIN", props.getProperty("sasl.mechanism"));
        assertNull(props.getProperty("does.not.exist"));
    }

    @Test
    void bootstrapServersFlagAlwaysWinsOverConfigFile(@TempDir Path tempDir) throws IOException {
        Path configFile = tempDir.resolve("client.properties");
        Files.writeString(configFile, "bootstrap.servers=other-cluster:9092\n");

        Properties props = ClientConfigLoader.load("localhost:9092", configFile);

        assertEquals("localhost:9092", props.getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    void missingConfigFileThrows(@TempDir Path tempDir) {
        Path missing = tempDir.resolve("does-not-exist.properties");
        assertThrows(IllegalArgumentException.class, () -> ClientConfigLoader.load("localhost:9092", missing));
    }
}
