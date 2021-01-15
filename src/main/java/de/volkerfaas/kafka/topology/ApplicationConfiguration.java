package de.volkerfaas.kafka.topology;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.ulisesbocchio.jasyptspringboot.annotation.EnableEncryptableProperties;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import org.apache.kafka.clients.admin.AdminClient;
import org.jasypt.encryption.StringEncryptor;
import org.jasypt.encryption.pbe.PooledPBEStringEncryptor;
import org.jasypt.encryption.pbe.config.SimpleStringPBEConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.*;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@EnableCaching
@ComponentScan(basePackages = { "de.volkerfaas.kafka.topology", "de.volkerfaas.kafka.cluster" })
@EnableEncryptableProperties
@PropertySource(name = "clusterConfig", value = "${config.resource}/${cluster}.yaml", ignoreResourceNotFound = true)
public class ApplicationConfiguration {

    @Component
    @ConfigurationProperties(prefix = "cluster")
    public static class ClusterProperties {

        private Map<String, String> kafka;
        private Map<String, String> schemaRegistry;

        public Map<String, String> getKafka() {
            return kafka;
        }

        public void setKafka(final Map<String, String> kafka) {
            this.kafka = kafka;
        }

        public Map<String, String> getSchemaRegistry() {
            return schemaRegistry;
        }

        public void setSchemaRegistry(final Map<String, String> schemaRegistry) {
            this.schemaRegistry = schemaRegistry;
        }

    }

    public static final String DIRECTORY_DELIMITER = "/";
    public static final String EVENTS_DIRECTORY = "events";
    public static final String REGEX_PRINCIPAL = "^(User)+\\:([0-9]+)*$";
    public static final String REGEX_DOMAIN = "([a-z]+\\.[a-z]+\\.[a-z_]+)";
    public static final String REGEX_VISIBILITY = "(public|protected|private)";
    public static final String REGEX_TOPIC_NAME = "([a-z]+[a-z_]+)";
    public static final String REGEX_TOPIC_VERSION = "(\\.([0-9]+)){0,1}";
    public static final String REGEX_FULL_TOPIC_NAME = REGEX_DOMAIN + "\\." + REGEX_VISIBILITY + "\\." + REGEX_TOPIC_NAME + REGEX_TOPIC_VERSION;
    public static final String REGEX_SCHEMA_SUBJECT = REGEX_FULL_TOPIC_NAME + "\\-(key|value)";
    public static final String REGEX_SCHEMA_FILENAME = "(" + REGEX_SCHEMA_SUBJECT + ")\\.(avsc|proto|json)";
    public static final String REGEX_SCHEMA_PATH_RELATIVE = "^.*?" + EVENTS_DIRECTORY + DIRECTORY_DELIMITER + REGEX_DOMAIN + DIRECTORY_DELIMITER + REGEX_SCHEMA_FILENAME + "$";
    public static final String REGEX_TOPOLOGY_FILENAME = "topology\\-" + REGEX_DOMAIN + "\\.yaml";
    public static final String TOPIC_CONFIG_KEY_CLEANUP_POLICY = "cleanup.policy";

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
    public AdminClient adminClient(@Autowired final ClusterProperties properties) {
        return AdminClient.create(new HashMap<>(properties.kafka));
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
    public SchemaRegistryClient schemaRegistryClient(@Autowired final ClusterProperties properties) {
        final Map<String, Object> originals = new HashMap<>(properties.schemaRegistry);
        final String schemaRegistryUrl = originals.get("schema.registry.url").toString();
        final RestService restService = new RestService(schemaRegistryUrl);
        List<SchemaProvider> providers = List.of(new AvroSchemaProvider(), new ProtobufSchemaProvider(), new JsonSchemaProvider());

        return new CachedSchemaRegistryClient(restService, 10, providers, originals, null);
    }

    @Bean
    public StringEncryptor jasyptStringEncryptor(@Value("${cluster}") final String cluster) {
        final PooledPBEStringEncryptor encryptor = new PooledPBEStringEncryptor();
        final SimpleStringPBEConfig config = new SimpleStringPBEConfig();
        config.setPassword(cluster);
        config.setAlgorithm("PBEWITHHMACSHA512ANDAES_256");
        config.setKeyObtentionIterations("1000");
        config.setPoolSize("1");
        config.setProviderName("SunJCE");
        config.setSaltGeneratorClassName("org.jasypt.salt.RandomSaltGenerator");
        config.setIvGeneratorClassName("org.jasypt.iv.RandomIvGenerator");
        config.setStringOutputType("base64");
        encryptor.setConfig(config);

        return encryptor;
    }

}
