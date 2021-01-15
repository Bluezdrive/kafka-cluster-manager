package de.volkerfaas.kafka.topology.bootstrap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;

import java.util.regex.Pattern;

public class PropertiesValidation {

    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesValidation.class);
    private static final String REGEX_BOOTSTRAP_SERVER = "^[a-z][a-z0-9-.]+:[0-9]+$";
    private static final String REGEX_SCHEMA_REGISTRY_URL = "^https://[a-z][a-z0-9-.]+$";
    private static final Pattern PATTERN_BOOTSTRAP_SERVER = Pattern.compile(REGEX_BOOTSTRAP_SERVER);
    private static final Pattern PATTERN_SCHEMA_REGISTRY_URL = Pattern.compile(REGEX_SCHEMA_REGISTRY_URL);

    private final Environment environment;
    private boolean result;

    public static PropertiesValidation init(final Environment environment) {
        return new PropertiesValidation(environment);
    }

    private PropertiesValidation(final Environment environment) {
        this.environment = environment;
        this.result = true;
    }

    public PropertiesValidation validateProperties() {
        final boolean notValidBootstrapServerUrl = isNotValidUrl("BOOTSTRAP_SERVER", PATTERN_BOOTSTRAP_SERVER, "must be a valid URL for an Apache Kafka® bootstrap server");
        final boolean notValidClusterApiKey = matchesNullOrEmpty("CLUSTER_API_KEY", "must be a valid API Key");
        final boolean notValidClusterApiSecret = matchesNullOrEmpty("CLUSTER_API_SECRET", "must be a valid API Secret");
        final boolean notValidSchemaRegistryUrl = isNotValidUrl("SCHEMA_REGISTRY_URL", PATTERN_SCHEMA_REGISTRY_URL, "must be a valid URL for an Confluent Schema Registry");
        final boolean notValidSchemaRegistryApiKey = matchesNullOrEmpty("SCHEMA_REGISTRY_API_KEY", "must be a valid API Key");
        final boolean notValidSchemaRegistryApiSecret = matchesNullOrEmpty("SCHEMA_REGISTRY_API_SECRET", "must be a valid API Secret");
        if (notValidBootstrapServerUrl
                || notValidClusterApiKey
                || notValidClusterApiSecret
                || notValidSchemaRegistryUrl
                || notValidSchemaRegistryApiKey
                || notValidSchemaRegistryApiSecret) {
            this.result = false;
        }

        return this;
    }

    public boolean result() {
        return result;
    }

    public boolean isNotValidUrl(final String key, final Pattern pattern, final String message) {
        final String value = environment.getProperty(key);
        final boolean notValidUrl = StringUtils.isEmpty(value) || !pattern.matcher(value).matches();
        if (notValidUrl) {
            LOGGER.error("Property '{}' {}", key, message);
        }
        return notValidUrl;
    }

    public boolean matchesNullOrEmpty(final String key, final String message) {
        final String value = environment.getProperty(key);
        final boolean matchesNullOrEmpty = StringUtils.isEmpty(value);
        if (matchesNullOrEmpty) {
            LOGGER.error("Property '{}' {}", key, message);
        }
        return matchesNullOrEmpty;
    }

}
