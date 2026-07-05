package io.github.borkorajkovic.kafkactl.kafka;

import org.apache.kafka.clients.admin.AdminClientConfig;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Builds the {@link Properties} passed to {@code AdminClient.create}, merging
 * an optional client config file (e.g. {@code security.protocol},
 * {@code sasl.mechanism}, {@code sasl.jaas.config} for SASL/SSL auth) with the
 * {@code --bootstrap-server} value, which always wins so the file cannot
 * silently redirect the tool to a different cluster.
 */
public final class ClientConfigLoader {

    private ClientConfigLoader() {
    }

    public static Properties load(String bootstrapServers, Path configFile) throws IOException {
        Properties props = new Properties();

        if (configFile != null) {
            if (!Files.isRegularFile(configFile) || !Files.isReadable(configFile)) {
                throw new IllegalArgumentException("Config file not readable: " + configFile);
            }
            try (InputStream in = Files.newInputStream(configFile)) {
                props.load(in);
            }
        }

        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return props;
    }
}
