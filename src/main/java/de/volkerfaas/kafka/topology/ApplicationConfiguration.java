package de.volkerfaas.kafka.topology;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.env.Environment;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableCaching
public class ApplicationConfiguration {

    public static final String DIRECTORY_DELIMITER = "/";
    public static final String PROPERTY_KEY_TOPOLOGY_DIRECTORY = "kafka.topology.directory";
    public static final String PROPERTY_KEY_TOPOLOGY_ALLOW_DELETE = "kafka.topology.allow-delete";
    public static final String PROPERTY_KEY_TOPOLOGY_DOMAINS = "kafka.topology.domains";
    public static final String PROPERTY_KEY_TOPOLOGY_DRY_RUN = "kafka.topology.dry-run";
    public static final String PROPERTY_KEY_BOOTSTRAP_SERVERS = "kafka.bootstrap-servers";
    public static final String PROPERTY_KEY_REQUEST_TIMEOUT_MS = "kafka.request.timeout.ms";
    public static final String PROPERTY_KEY_RETRY_BACKOFF_MS = "kafka.retry.backoff.ms";
    public static final String PROPERTY_KEY_SECURITY_PROTOCOL = "kafka.security.protocol";
    public static final String PROPERTY_KEY_SASL_MECHANISM = "kafka.sasl.mechanism";
    public static final String PROPERTY_KEY_SASL_JAAS_CONFIG = "kafka.sasl.jaas.config";
    public static final String PROPERTY_KEY_SCHEMA_REGISTRY_URL = "schema.registry.url";
    public static final String PROPERTY_KEY_SCHEMA_REGISTRY_BASIC_AUTH_CREDENTIALS_SOURCE = "schema.registry.basic.auth.credentials.source";
    public static final String PROPERTY_KEY_SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO = "schema.registry.basic.auth.user.info";

    public static final String SCHEMA_FILE_KEY_SUFFIX = "-key.avsc";
    public static final String SCHEMA_FILE_VALUE_SUFFIX = "-value.avsc";
    public static final String EVENTS_DIRECTORY = "events";

    public static final String REGEX_DOMAIN = "([a-z]+\\.[a-z]+\\.[_a-z]+)";
    public static final String REGEX_VISIBILITY = "(public|protected|private)";
    public static final String REGEX_TOPIC_NAME = "([a-z_]+)";
    public static final String REGEX_FULL_TOPIC_NAME = REGEX_DOMAIN + "\\." + REGEX_VISIBILITY + "\\." + REGEX_TOPIC_NAME;
    public static final String REGEX_SCHEMA_SUBJECT = REGEX_FULL_TOPIC_NAME + "\\-(key|value)";
    public static final String REGEX_SCHEMA_FILENAME = "(" + REGEX_SCHEMA_SUBJECT + ").avsc";
    public static final String REGEX_SCHEMA_PATH_RELATIVE = "^.*?" + EVENTS_DIRECTORY + DIRECTORY_DELIMITER + REGEX_DOMAIN + DIRECTORY_DELIMITER + REGEX_SCHEMA_FILENAME + "$";
    public static final String REGEX_TOPOLOGY_FILENAME = "topology\\-" + REGEX_DOMAIN + "\\.yaml";

    public static final String TOPIC_CONFIG_KEY_CLEANUP_POLICY = "cleanup.policy";

    private final Environment environment;

    @Autowired
    public ApplicationConfiguration(Environment environment) {
        this.environment = environment;
    }

    @Bean
    public CacheManager cacheManager() {
        return new ConcurrentMapCacheManager("cluster");
    }

    @Lazy
    @Bean
    public AdminClient adminClient() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty(PROPERTY_KEY_BOOTSTRAP_SERVERS));
        conf.put(AdminClientConfig.CLIENT_ID_CONFIG, "confluent-cloud-topology-builder");
        conf.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, environment.getProperty(PROPERTY_KEY_REQUEST_TIMEOUT_MS));
        conf.put(AdminClientConfig.RETRIES_CONFIG, "3");
        conf.put(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, environment.getProperty(PROPERTY_KEY_RETRY_BACKOFF_MS));
        conf.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, environment.getProperty(PROPERTY_KEY_SECURITY_PROTOCOL));
        conf.put(SaslConfigs.SASL_MECHANISM, environment.getProperty(PROPERTY_KEY_SASL_MECHANISM));
        conf.put(SaslConfigs.SASL_JAAS_CONFIG, environment.getProperty(PROPERTY_KEY_SASL_JAAS_CONFIG));

        return AdminClient.create(conf);
    }

    @Lazy
    @Bean
    public SchemaRegistryClient schemaRegistryClient() {
        final String schemaRegistryUrl = environment.getProperty(PROPERTY_KEY_SCHEMA_REGISTRY_URL, "");
        final Map<String, String> originals = new HashMap<>();
        originals.put(PROPERTY_KEY_SCHEMA_REGISTRY_URL, schemaRegistryUrl);

        final String basicAuthCredentialsSource = environment.getProperty(PROPERTY_KEY_SCHEMA_REGISTRY_BASIC_AUTH_CREDENTIALS_SOURCE);
        if (basicAuthCredentialsSource != null && !basicAuthCredentialsSource.isBlank()) {
            originals.put("basic.auth.credentials.source", basicAuthCredentialsSource);
        }
        final String basicAuthUserInfo = environment.getProperty(PROPERTY_KEY_SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO);
        if (basicAuthUserInfo != null && !basicAuthUserInfo.isBlank()) {
            originals.put(PROPERTY_KEY_SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO, basicAuthUserInfo);
        }

        return new CachedSchemaRegistryClient(schemaRegistryUrl, 10, originals);
    }

}
