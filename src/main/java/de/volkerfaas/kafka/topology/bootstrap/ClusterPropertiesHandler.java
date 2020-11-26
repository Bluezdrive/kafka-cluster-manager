package de.volkerfaas.kafka.topology.bootstrap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Objects;
import java.util.Properties;

public final class ClusterPropertiesHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterPropertiesHandler.class);

    private final String fileName;

    public ClusterPropertiesHandler(final String fileName) {
        this.fileName = fileName;
    }

    public String getFileName() {
        return fileName;
    }

    public ClusterPropertiesHandler loadClusterProperties() throws IOException {
        if (Objects.isNull(fileName)) {
            LOGGER.info("Using environment variables to connect to cluster");
        } else {
            final Properties properties = new Properties();
            final File file = new File("conf", fileName);
            try (InputStream input = new FileInputStream(file)) {
                properties.load(input);
                properties.forEach((key, value) -> System.setProperty(key.toString(), value.toString()));
                LOGGER.info("Using configuration file '{}' to connect to cluster", fileName);
            } catch (FileNotFoundException e) {
                LOGGER.error("Configuration file '{}' not found", fileName);
                System.exit(1);
            }
        }
        return this;
    }

    public void verifyClusterPropertiesSet() {
        boolean result = isSystemPropertySet("BOOTSTRAP_SERVER", false);
        result = isSystemPropertySet("CLUSTER_API_KEY", true) && result;
        result = isSystemPropertySet("CLUSTER_API_SECRET", true) && result;
        result = isSystemPropertySet("SCHEMA_REGISTRY_URL", false) && result;
        result = isSystemPropertySet("SCHEMA_REGISTRY_API_KEY", true) && result;
        result = isSystemPropertySet("SCHEMA_REGISTRY_API_SECRET", true) && result;
        if (!result) {
            System.exit(1);
        }
    }

    public boolean isSystemPropertySet(final String key, final boolean secret) {
        String value = System.getProperty(key);
        if (Objects.isNull(value)) {
            value = System.getenv(key);
        }
        if (Objects.isNull(value) || value.isBlank()) {
            LOGGER.error(" Property '{}' not set!", key);

            return false;
        }
        LOGGER.info(" {}={}", key, (secret ? "[secret]" : value));

        return true;
    }

}
