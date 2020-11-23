package de.volkerfaas.kafka.topology;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.Environment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@EnableCaching
@ComponentScan(basePackages = { "de.volkerfaas.kafka.topology", "de.volkerfaas.kafka.cluster" })
public class ApplicationConfiguration {

    public static final String DIRECTORY_DELIMITER = "/";
    public static final String PROPERTY_KEY_BOOTSTRAP_SERVERS = "kafka.bootstrap-servers";
    public static final String PROPERTY_KEY_REQUEST_TIMEOUT_MS = "kafka.request.timeout.ms";
    public static final String PROPERTY_KEY_RETRY_BACKOFF_MS = "kafka.retry.backoff.ms";
    public static final String PROPERTY_KEY_SECURITY_PROTOCOL = "kafka.security.protocol";
    public static final String PROPERTY_KEY_SASL_MECHANISM = "kafka.sasl.mechanism";
    public static final String PROPERTY_KEY_SASL_JAAS_CONFIG = "kafka.sasl.jaas.config";
    public static final String PROPERTY_KEY_SCHEMA_REGISTRY_URL = "schema.registry.url";
    public static final String PROPERTY_KEY_SCHEMA_REGISTRY_BASIC_AUTH_CREDENTIALS_SOURCE = "schema.registry.basic.auth.credentials.source";
    public static final String PROPERTY_KEY_SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO = "schema.registry.basic.auth.user.info";

    public static final String EVENTS_DIRECTORY = "events";
    public static final String REGEX_PRINCIPAL = "^(User)+\\:([0-9]+)*$";
    public static final String REGEX_DOMAIN = "([a-z]+\\.[a-z]+\\.[a-z_]+)";
    public static final String REGEX_VISIBILITY = "(public|protected|private)";
    public static final String REGEX_TOPIC_NAME = "([a-z]+[a-z_]+)";
    public static final String REGEX_FULL_TOPIC_NAME = REGEX_DOMAIN + "\\." + REGEX_VISIBILITY + "\\." + REGEX_TOPIC_NAME;
    public static final String REGEX_SCHEMA_SUBJECT = REGEX_FULL_TOPIC_NAME + "\\-(key|value)";
    public static final String REGEX_SCHEMA_FILENAME = "(" + REGEX_SCHEMA_SUBJECT + ")\\.(avsc|proto|json)";
    public static final String REGEX_SCHEMA_PATH_RELATIVE = "^.*?" + EVENTS_DIRECTORY + DIRECTORY_DELIMITER + REGEX_DOMAIN + DIRECTORY_DELIMITER + REGEX_SCHEMA_FILENAME + "$";
    public static final String REGEX_TOPOLOGY_FILENAME = "topology\\-" + REGEX_DOMAIN + "\\.yaml";

    public static final String TOPIC_CONFIG_KEY_CLEANUP_POLICY = "cleanup.policy";

    private final Environment environment;

    @Autowired
    public ApplicationConfiguration(final Environment environment) {
        this.environment = environment;
    }

    @Bean
    public CacheManager cacheManager() {
        return new ConcurrentMapCacheManager("cluster");
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        final PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer = new PropertySourcesPlaceholderConfigurer();
        propertySourcesPlaceholderConfigurer.setNullValue("@null");

        return propertySourcesPlaceholderConfigurer;
    }

    @Lazy
    @Bean
    public AdminClient adminClient() {
        final Map<String, Object> conf = new HashMap<>();
        conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty(PROPERTY_KEY_BOOTSTRAP_SERVERS));
        conf.put(AdminClientConfig.CLIENT_ID_CONFIG, "kafka-cluster-manager");
        conf.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, environment.getProperty(PROPERTY_KEY_REQUEST_TIMEOUT_MS));
        conf.put(AdminClientConfig.RETRIES_CONFIG, "3");
        conf.put(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, environment.getProperty(PROPERTY_KEY_RETRY_BACKOFF_MS));
        conf.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, environment.getProperty(PROPERTY_KEY_SECURITY_PROTOCOL));
        conf.put(SaslConfigs.SASL_MECHANISM, environment.getProperty(PROPERTY_KEY_SASL_MECHANISM));
        conf.put(SaslConfigs.SASL_JAAS_CONFIG, environment.getProperty(PROPERTY_KEY_SASL_JAAS_CONFIG));

        return AdminClient.create(conf);
    }

    @Bean
    public ObjectMapper objectMapper() {
        final YAMLFactory yamlFactory = new YAMLFactory()
                .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                .disable(YAMLGenerator.Feature.SPLIT_LINES)
                .disable(YAMLGenerator.Feature.INDENT_ARRAYS)
                .disable(YAMLGenerator.Feature.CANONICAL_OUTPUT)
                .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES);
        final ObjectMapper objectMapper = new ObjectMapper(yamlFactory);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        objectMapper.findAndRegisterModules();

        return objectMapper;
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
        final RestService restService = new RestService(schemaRegistryUrl);
        List<SchemaProvider> providers = List.of(new AvroSchemaProvider(), new ProtobufSchemaProvider(), new JsonSchemaProvider());

        return new CachedSchemaRegistryClient(restService, 10, providers, originals, null);
    }

}
