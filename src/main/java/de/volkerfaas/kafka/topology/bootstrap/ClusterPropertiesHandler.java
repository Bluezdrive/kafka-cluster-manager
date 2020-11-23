package de.volkerfaas.kafka.topology.bootstrap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Objects;
import java.util.Properties;

public final class ClusterPropertiesHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterPropertiesHandler.class);

    private final String name;

    public ClusterPropertiesHandler() {
        this(null);
    }

    public ClusterPropertiesHandler(final String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public ClusterPropertiesHandler loadClusterProperties() throws IOException {
        final File file = new File("conf/", name + ".cluster");
        final Properties properties = new Properties();
        try (InputStream input = new FileInputStream(file)) {
            properties.load(input);
            properties.forEach((key, value) -> System.setProperty(key.toString(), value.toString()));
            LOGGER.info("Using configuration file for cluster '{}'", name);
        } catch (FileNotFoundException e) {
            LOGGER.warn("No configuration file found for cluster '{}'", name);
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
