package de.volkerfaas.kafka.topology;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.volkerfaas.kafka.topology.model.*;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.acl.AclBinding;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static de.volkerfaas.kafka.topology.utils.MockUtils.*;
import static de.volkerfaas.kafka.topology.utils.TestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@SpringBootTest(properties = {"documentation.filename=topology.md"})
@ContextConfiguration(loader= AnnotationConfigContextLoader.class)
@ExtendWith(SpringExtension.class)
@DisplayName("The kafka topology manager")
class RestoreTopologyIntegrationTest {

    @Autowired
    private CacheManager cacheManager;

    @Autowired
    private ConfigurableEnvironment environment;

    @Autowired
    private KafkaClusterManager kafkaClusterManager;

    @MockBean
    private AdminClient adminClient;

    @MockBean
    private SchemaRegistryClient schemaRegistryClient;

    @MockBean
    private ObjectMapper objectMapper;

    private String topologyDirectory;

    @TestConfiguration
    public static class ContextConfiguration {

        @Bean
        public CacheManager cacheManager() {
            return new ConcurrentMapCacheManager("cluster");
        }

    }

    @BeforeEach
    void init() throws JsonProcessingException {
        final URL resource = getClass().getClassLoader().getResource("topology-de.volkerfaas.arc.yaml");
        assertNotNull(resource);
        final File topologyFile = new File(resource.getPath());
        this.topologyDirectory = topologyFile.getParent();
        doCallRealMethod().when(objectMapper).readValue(anyString(), any(Class.class));
    }

    @AfterEach
    void destroy() {
        Objects.requireNonNull(cacheManager.getCache("cluster")).clear();
        reset(adminClient, schemaRegistryClient);
    }

    @Nested
    @DisplayName("should restore the topology")
    class RestoreTopology {

        @BeforeEach
        void init() {
            final Properties properties = new Properties();
            properties.put(ApplicationConfiguration.PROPERTY_KEY_TOPOLOGY_DIRECTORY, topologyDirectory);
            System.setProperty(ApplicationConfiguration.PROPERTY_KEY_TOPOLOGY_DIRECTORY, topologyDirectory);
            properties.put(ApplicationConfiguration.PROPERTY_KEY_TOPOLOGY_DRY_RUN, false);
            final PropertiesPropertySource propertySource = new PropertiesPropertySource("confluent-cloud-topology-builder", properties);
            environment.getPropertySources().addFirst(propertySource);
        }

        @Test
        @DisplayName("into the corresponding file for the domain")
        void restoreTopology() throws ExecutionException, InterruptedException, IOException, RestClientException {
            final String domainName = "de.volkerfaas.music";
            final String topicName = domainName + ".public.sound_played";
            final String domainPrincipal = "User:142637";
            final Set<String> visibilityPrincipals = Set.of("User:138166", "User:933177");

            final Set<AclBinding> aclBindings = createDomainAclBindings("de.volkerfaas.music.", domainPrincipal);
            visibilityPrincipals.forEach(principal -> aclBindings.addAll(createConsumerAclBindings("de.volkerfaas.music.public.", principal)));

            mockDescribeCluster(adminClient, "lkc-p5zy2");
            mockListTopics(adminClient, Set.of(topicName));
            mockDescribeTopics(adminClient, new HashMap<>(createTopicPartitionInfos(topicName, 9, 9)));
            mockDescribeConfigs(adminClient, new HashMap<>(createConfig(topicName, Collections.emptySet())));
            mockListOffsets(adminClient, Collections.emptyMap());
            mockDescribeAcls(adminClient, aclBindings);
            mockListConsumerGroups(adminClient, Collections.emptyList());

            doReturn(List.of("de.volkerfaas.music.public.sound_played-value")).when(schemaRegistryClient).getAllSubjects();

            doAnswer(invocation -> {
                final File resultFile = invocation.getArgument(0);
                assertNotNull(resultFile);
                assertEquals("restore-de.volkerfaas.music.yaml", resultFile.getName());
                assertEquals(topologyDirectory, resultFile.getParent());
                final TopologyFile value = invocation.getArgument(1);
                assertNotNull(value);
                assertEquals("restore-de.volkerfaas.music.yaml", resultFile.getName());
                final Domain domain = value.getDomain();
                assertNotNull(domain);
                assertEquals(domainName, domain.getName());
                assertEquals(domainPrincipal, domain.getPrincipal());
                final List<Visibility> visibilities = domain.getVisibilities();
                assertNotNull(visibilities);
                assertEquals(1, visibilities.size());
                final Visibility visibility = visibilities.get(0);
                assertNotNull(visibility);
                assertEquals(Visibility.Type.PUBLIC, visibility.getType());
                final List<AccessControl> consumers = visibility.getConsumers();
                assertNotNull(consumers);
                assertEquals(2, consumers.size());
                assertEquals(visibilityPrincipals, consumers.stream().map(AccessControl::getPrincipal).collect(Collectors.toSet()));
                final List<Topic> topics = visibility.getTopics();
                assertNotNull(topics);
                assertEquals(1, topics.size());
                final Topic topic = topics.get(0);
                assertNotNull(topic);
                assertEquals("sound_played", topic.getName());
                assertEquals(9, topic.getNumPartitions());
                assertEquals(topicName, topic.getFullName());

                return null;
            }).when(objectMapper).writeValue(any(File.class), any(TopologyFile.class));

            final SchemaMetadata schemaMetadata = mock(SchemaMetadata.class);
            doReturn("{ \"type\": \"string\" }").when(schemaMetadata).getSchema();
            doReturn(schemaMetadata).when(schemaRegistryClient).getLatestSchemaMetadata(anyString());

            kafkaClusterManager.restoreTopology(topologyDirectory, List.of(domainName));
        }

    }

}
