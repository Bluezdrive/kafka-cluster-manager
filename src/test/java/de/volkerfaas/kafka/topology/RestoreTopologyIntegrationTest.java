package de.volkerfaas.kafka.topology;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.volkerfaas.kafka.topology.model.*;
import de.volkerfaas.kafka.topology.services.DocumentationService;
import de.volkerfaas.kafka.topology.services.TopologyBuildService;
import de.volkerfaas.kafka.topology.services.TopologyRestoreService;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.acl.AclBinding;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.cache.CacheManager;

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
import static org.mockito.Mockito.*;

@SpringBootTest(
        properties = {"documentation.filename=topology.md"},
        args = {"--cluster=test"}
)
@DisplayName("The Kafka Cluster Manager")
public class RestoreTopologyIntegrationTest {

    @Autowired
    private CacheManager cacheManager;

    @Autowired
    private DocumentationService documentationService;

    @Autowired
    private TopologyBuildService topologyBuildService;

    @Autowired
    private TopologyRestoreService topologyRestoreService;

    @MockBean
    private AdminClient adminClient;

    @MockBean
    private SchemaRegistryClient schemaRegistryClient;

    @MockBean
    private ObjectMapper objectMapper;

    private String topologyDirectory;

    @MockBean
    private KafkaClusterManager kafkaClusterManager;

    @BeforeAll()
    static void setup() {
        System.setProperty("BOOTSTRAP_SERVER", "localhost:9092");
        System.setProperty("CLUSTER_API_KEY", "test_api_key");
        System.setProperty("CLUSTER_API_SECRET", "test_api_secret");
        System.setProperty("SCHEMA_REGISTRY_URL", "https://localhost");
        System.setProperty("SCHEMA_REGISTRY_API_KEY", "test_api_key");
        System.setProperty("SCHEMA_REGISTRY_API_SECRET", "test_api_secret");
    }

    @BeforeEach
    void init() {
        final URL resource = getClass().getClassLoader().getResource("topology-de.volkerfaas.arc.yaml");
        assertNotNull(resource);
        this.topologyDirectory = new File(resource.getPath()).getParent();
        this.kafkaClusterManager = new KafkaClusterManager(documentationService, topologyBuildService, topologyRestoreService);
    }

    @AfterEach
    void destroy() {
        Objects.requireNonNull(cacheManager.getCache("cluster")).clear();
        reset(adminClient, schemaRegistryClient);
    }

    @Nested
    @DisplayName("should restore the topology")
    class RestoreTopology {

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
            doReturn("AVRO").when(schemaMetadata).getSchemaType();
            doReturn(schemaMetadata).when(schemaRegistryClient).getLatestSchemaMetadata(anyString());
            doReturn("FORWARD_TRANSITIVE").when(schemaRegistryClient).getCompatibility(anyString());

            String[] args = new String[]{"--cluster=test", "--directory=" + topologyDirectory, "--restore", "--domain=" + domainName};
            kafkaClusterManager.run(args);
        }

        @Test
        @DisplayName("without content when domain is not set")
        void restoreTopologyWithoutDomain() throws ExecutionException, InterruptedException, IOException, RestClientException {
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

            final SchemaMetadata schemaMetadata = mock(SchemaMetadata.class);
            doReturn("{ \"type\": \"string\" }").when(schemaMetadata).getSchema();
            doReturn("AVRO").when(schemaMetadata).getSchemaType();
            doReturn(schemaMetadata).when(schemaRegistryClient).getLatestSchemaMetadata(anyString());
            doReturn("FORWARD_TRANSITIVE").when(schemaRegistryClient).getCompatibility(anyString());

            String[] args = new String[]{"--cluster=test", "--directory=" + topologyDirectory, "--restore"};
            kafkaClusterManager.run(args);
            verify(objectMapper, never()).writeValue(any(File.class), any(TopologyFile.class));
        }

    }

}
