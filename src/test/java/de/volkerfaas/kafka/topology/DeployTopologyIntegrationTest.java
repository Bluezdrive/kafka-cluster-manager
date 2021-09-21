package de.volkerfaas.kafka.topology;

import de.volkerfaas.kafka.topology.services.*;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.*;
import org.mockito.invocation.InvocationOnMock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.cache.CacheManager;

import java.io.File;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static de.volkerfaas.kafka.topology.utils.MatcherUtils.hasEntryWithIterableValue;
import static de.volkerfaas.kafka.topology.utils.MockUtils.*;
import static de.volkerfaas.kafka.topology.utils.TestUtils.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static uk.org.webcompere.systemstubs.SystemStubs.catchSystemExit;

@SpringBootTest(
        properties = {"documentation.filename=topology.md"},
        args = {"--cluster=test"}
)
@DisplayName("The Kafka Cluster Manager")
public class DeployTopologyIntegrationTest {

    @Autowired
    private CacheManager cacheManager;

    @Autowired
    private DocumentationService documentationService;

    @Autowired
    private TopologyDeployService topologyDeployService;

    @Autowired
    private TopologyCreateService topologyCreateService;

    @Autowired
    private TopologyDeleteService topologyDeleteService;

    @Autowired
    private TopologyRestoreService topologyRestoreService;

    @MockBean
    private KafkaClusterManager kafkaClusterManager;

    @MockBean
    private AdminClient adminClient;

    @MockBean
    private SchemaRegistryClient schemaRegistryClient;

    private String topologyDirectory;

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
        this.kafkaClusterManager = new KafkaClusterManager(documentationService, topologyDeployService, topologyCreateService, topologyRestoreService, topologyDeleteService);
    }

    @AfterEach
    void destroy() {
        Objects.requireNonNull(cacheManager.getCache("cluster")).clear();
        reset(adminClient, schemaRegistryClient);
    }

    @Nested
    @DisplayName("should build the topology")
    class BuildTopology {

        @Test
        @DisplayName("without changes to cluster when topology equals cluster")
        void testTopologyEqualsCluster() throws ExecutionException, InterruptedException {
            final String topicNameUserUpdated = "de.volkerfaas.arc.public.user_updated";
            final String topicNameTestCreated = "de.volkerfaas.test.public.test_created";

            final Set<AclBinding> aclBindings = createDomainAclBindings("de.volkerfaas.arc.", "User:129849");
            aclBindings.addAll(createDomainAclBindings("de.volkerfaas.test.", "User:138166"));
            aclBindings.addAll(createConsumerAclBindings("de.volkerfaas.arc.public.", "User:138166"));

            final Map<String, TopicDescription> topicDescriptions = new HashMap<>();
            topicDescriptions.putAll(createTopicPartitionInfos(topicNameUserUpdated, 5, 9));
            topicDescriptions.putAll(createTopicPartitionInfos(topicNameTestCreated, 3, 9));

            final Map<ConfigResource, Config> configs = new HashMap<>();
            configs.putAll(createConfig(topicNameUserUpdated, Set.of(
                    ConfigEntryUtils.createDynamicTopicConfigEntry("cleanup.policy", "compact"),
                    ConfigEntryUtils.createDynamicTopicConfigEntry("min.compaction.lag.ms", "100")
            )));
            configs.putAll(createConfig(topicNameTestCreated, Collections.emptySet()));

            mockDescribeAcls(adminClient, aclBindings);
            mockDescribeCluster(adminClient, "lkc-p5zy2");
            mockDescribeConfigs(adminClient, configs);
            mockDescribeTopics(adminClient, topicDescriptions);
            mockListTopics(adminClient, Set.of(topicNameUserUpdated, topicNameTestCreated));
            mockParseSchema(schemaRegistryClient, "{ \"type\": \"string\" }");
            mockListOffsets(adminClient, Collections.emptyMap());
            mockListConsumerGroups(adminClient, Collections.emptyList());
            mockDescribeConsumerGroups(adminClient, Collections.emptyMap());

            String[] args = new String[]{"--cluster=test", "--directory=" + topologyDirectory};
            kafkaClusterManager.run(args);
            verify(adminClient, never()).createTopics(any());
            verify(adminClient, never()).createPartitions(any());
            verify(adminClient, never()).createAcls(any());
            verify(adminClient, never()).deleteAcls(any());
            verify(adminClient, never()).incrementalAlterConfigs(any());
        }

        @Test
        @DisplayName("with one new partition being created when partitions in cluster are less than partitions in topology")
        void testNewPartition() throws InterruptedException, ExecutionException {
            final String topicNameUserUpdated = "de.volkerfaas.arc.public.user_updated";
            final String topicNameTestCreated = "de.volkerfaas.test.public.test_created";

            final Set<AclBinding> aclBindings = createDomainAclBindings("de.volkerfaas.arc.", "User:129849");
            aclBindings.addAll(createDomainAclBindings("de.volkerfaas.test.", "User:138166"));
            aclBindings.addAll(createConsumerAclBindings("de.volkerfaas.arc.public.", "User:138166"));

            final Map<String, TopicDescription> topicDescriptions = new HashMap<>();
            topicDescriptions.putAll(createTopicPartitionInfos(topicNameUserUpdated, 4, 9));
            topicDescriptions.putAll(createTopicPartitionInfos(topicNameTestCreated, 3, 9));

            final Map<ConfigResource, Config> configs = new HashMap<>();
            configs.putAll(createConfig(topicNameUserUpdated, Set.of(
                    ConfigEntryUtils.createDynamicTopicConfigEntry("cleanup.policy", "compact"),
                    ConfigEntryUtils.createDynamicTopicConfigEntry("min.compaction.lag.ms", "100")
            )));
            configs.putAll(createConfig(topicNameTestCreated, Collections.emptySet()));

            mockDescribeAcls(adminClient, aclBindings);
            mockDescribeCluster(adminClient, "lkc-p5zy2");
            mockDescribeConfigs(adminClient, configs);
            mockDescribeTopics(adminClient, topicDescriptions);
            mockListTopics(adminClient, Set.of(topicNameUserUpdated, topicNameTestCreated));
            mockParseSchema(schemaRegistryClient, "{ \"type\": \"string\" }");
            mockListOffsets(adminClient, Collections.emptyMap());
            mockListConsumerGroups(adminClient, Collections.emptyList());
            mockDescribeConsumerGroups(adminClient, Collections.emptyMap());

            final KafkaFuture<Void> kafkaFuture = mock(KafkaFuture.class);
            doReturn(null).when(kafkaFuture).get();
            final CreatePartitionsResult createPartitionsResult = mock(CreatePartitionsResult.class);
            doReturn(kafkaFuture).when(createPartitionsResult).all();
            doAnswer(invocation -> {
                final Map<String, NewPartitions> newPartitions = invocation.getArgument(0);
                assertNotNull(newPartitions);
                assertEquals(1, newPartitions.size());
                assertTrue(newPartitions.containsKey(topicNameUserUpdated));
                assertNotNull(newPartitions.get(topicNameUserUpdated));

                return createPartitionsResult;
            }).when(adminClient).createPartitions(anyMap());

            String[] args = new String[]{"--cluster=test", "--directory=" + topologyDirectory};
            kafkaClusterManager.run(args);
            verify(adminClient, never()).createTopics(any());
            verify(adminClient, never()).createAcls(any());
            verify(adminClient, never()).deleteAcls(any());
            verify(adminClient, never()).incrementalAlterConfigs(any());
        }

        @Test
        @DisplayName("with one new topic being created when it doesn't exist in cluster")
        void testNewTopic() throws ExecutionException, InterruptedException {
            final String topicNameUserUpdated = "de.volkerfaas.arc.public.user_updated";
            final String topicNameTestCreated = "de.volkerfaas.test.public.test_created";
            final int numPartitionsTestCreated = 3;
            final short replicationFactorTestCreated = 3;

            mockDescribeAcls(adminClient, createDomainAclBindings("de.volkerfaas.arc.", "User:129849"));
            mockDescribeCluster(adminClient, "lkc-p5zy2");
            mockDescribeConfigs(adminClient, createConfig(topicNameUserUpdated, Set.of(
                    ConfigEntryUtils.createDynamicTopicConfigEntry("cleanup.policy", "compact"),
                    ConfigEntryUtils.createDynamicTopicConfigEntry("min.compaction.lag.ms", "100")
            )));
            mockDescribeTopics(adminClient, createTopicPartitionInfos(topicNameUserUpdated, 5, 9));
            mockListTopics(adminClient, Set.of(topicNameUserUpdated));
            mockParseSchema(schemaRegistryClient, "{ \"type\": \"string\" }");
            mockListOffsets(adminClient, Collections.emptyMap());
            mockListConsumerGroups(adminClient, Collections.emptyList());
            mockDescribeConsumerGroups(adminClient, Collections.emptyMap());

            final KafkaFuture<Void> kafkaFuture = mock(KafkaFuture.class);
            doReturn(null).when(kafkaFuture).get();
            final CreateAclsResult createAclsResult = mock(CreateAclsResult.class);
            doReturn(kafkaFuture).when(createAclsResult).all();
            final Set<AclBinding> expectedAclBindings = createDomainAclBindings("de.volkerfaas.test.", "User:138166");
            expectedAclBindings.addAll(createConsumerAclBindings("de.volkerfaas.arc.public.", "User:138166"));
            doAnswer(invocation -> {
                final Set<AclBinding> acls = invocation.getArgument(0);
                assertNotNull(acls);
                assertEquals(9, acls.size());
                assertThat(acls, containsInAnyOrder(expectedAclBindings.toArray()));

                return createAclsResult;
            }).when(adminClient).createAcls(anyCollection());

            final CreateTopicsResult createTopicsResult = mock(CreateTopicsResult.class);
            doReturn(kafkaFuture).when(createTopicsResult).all();
            doAnswer(invocation -> {
                final Set<NewTopic> newTopics = invocation.getArgument(0);
                assertNotNull(newTopics);
                assertEquals(1, newTopics.size());
                assertThat(newTopics, containsInAnyOrder(
                        new NewTopic(topicNameTestCreated, numPartitionsTestCreated, replicationFactorTestCreated).configs(new HashMap<>())
                ));

                return createTopicsResult;
            }).when(adminClient).createTopics(anySet());

            String[] args = new String[]{"--cluster=test", "--directory=" + topologyDirectory};
            kafkaClusterManager.run(args);
            verify(adminClient, never()).createPartitions(any());
            verify(adminClient, never()).deleteAcls(any());
            verify(adminClient, never()).incrementalAlterConfigs(any());
        }

        @Test
        @DisplayName("with config items being set when they don't exist in cluster")
        void testConfigChanged() throws InterruptedException, ExecutionException {
            final String topicNameUserUpdated = "de.volkerfaas.arc.public.user_updated";
            final String topicNameTestCreated = "de.volkerfaas.test.public.test_created";

            final Set<AclBinding> aclBindings = createDomainAclBindings("de.volkerfaas.arc.", "User:129849");
            aclBindings.addAll(createDomainAclBindings("de.volkerfaas.test.", "User:138166"));
            aclBindings.addAll(createConsumerAclBindings("de.volkerfaas.arc.public.", "User:138166"));

            final Map<String, TopicDescription> topicDescriptions = new HashMap<>();
            topicDescriptions.putAll(createTopicPartitionInfos(topicNameUserUpdated, 5, 9));
            topicDescriptions.putAll(createTopicPartitionInfos(topicNameTestCreated, 3, 9));

            final Map<ConfigResource, Config> configs = new HashMap<>();
            configs.putAll(createConfig(topicNameUserUpdated, Collections.emptySet()));
            configs.putAll(createConfig(topicNameTestCreated, Collections.emptySet()));

            mockDescribeAcls(adminClient, aclBindings);
            mockDescribeCluster(adminClient, "lkc-p5zy2");
            mockDescribeConfigs(adminClient, configs);
            mockDescribeTopics(adminClient, topicDescriptions);
            mockListTopics(adminClient, Set.of(topicNameUserUpdated, topicNameTestCreated));
            mockParseSchema(schemaRegistryClient, "{ \"type\": \"string\" }");
            mockListOffsets(adminClient, Collections.emptyMap());
            mockListConsumerGroups(adminClient, Collections.emptyList());
            mockDescribeConsumerGroups(adminClient, Collections.emptyMap());

            final KafkaFuture<Void> kafkaFuture = mock(KafkaFuture.class);
            doReturn(null).when(kafkaFuture).get();
            final AlterConfigsResult alterConfigsResult = mock(AlterConfigsResult.class);
            doReturn(kafkaFuture).when(alterConfigsResult).all();
            doAnswer(invocation -> {
                verifyConfig(topicNameUserUpdated, invocation);
                return alterConfigsResult;
            }).when(adminClient).incrementalAlterConfigs(anyMap());

            String[] args = new String[]{"--cluster=test", "--directory=" + topologyDirectory};
            kafkaClusterManager.run(args);
            verify(adminClient, never()).createTopics(any());
            verify(adminClient, never()).createPartitions(any());
            verify(adminClient, never()).createAcls(any());
            verify(adminClient, never()).deleteAcls(any());
        }

    }

    @Nested
    @DisplayName("should build and delete acls from the topology")
    class BuildTopologyWithAllowDeleteAcl {

        @Test
        @DisplayName("without changes to cluster when topology equals cluster")
        void testTopologyEqualsCluster() throws InterruptedException, ExecutionException {
            final String topicNameUserUpdated = "de.volkerfaas.arc.public.user_updated";
            final String topicNameTestCreated = "de.volkerfaas.test.public.test_created";

            final Set<AclBinding> aclBindings = createDomainAclBindings("de.volkerfaas.arc.", "User:129849");
            aclBindings.addAll(createDomainAclBindings("de.volkerfaas.test.", "User:138166"));
            aclBindings.addAll(createConsumerAclBindings("de.volkerfaas.arc.public.", "User:138166"));

            final Map<String, TopicDescription> topicDescriptions = new HashMap<>();
            topicDescriptions.putAll(createTopicPartitionInfos(topicNameUserUpdated, 5, 9));
            topicDescriptions.putAll(createTopicPartitionInfos(topicNameTestCreated, 3, 9));

            final Map<ConfigResource, Config> configs = new HashMap<>();
            configs.putAll(createConfig(topicNameUserUpdated, Set.of(
                    ConfigEntryUtils.createDynamicTopicConfigEntry("cleanup.policy", "compact"),
                    ConfigEntryUtils.createDynamicTopicConfigEntry("min.compaction.lag.ms", "100")
            )));
            configs.putAll(createConfig(topicNameTestCreated, Collections.emptySet()));

            mockDescribeAcls(adminClient, aclBindings);
            mockDescribeCluster(adminClient, "lkc-p5zy2");
            mockDescribeConfigs(adminClient, configs);
            mockDescribeTopics(adminClient, topicDescriptions);
            mockListTopics(adminClient, Set.of(topicNameUserUpdated, topicNameTestCreated));
            mockParseSchema(schemaRegistryClient, "{ \"type\": \"string\" }");
            mockListOffsets(adminClient, Collections.emptyMap());
            mockListConsumerGroups(adminClient, Collections.emptyList());
            mockDescribeConsumerGroups(adminClient, Collections.emptyMap());

            String[] args = new String[]{"--cluster=test", "--directory=" + topologyDirectory, "--allow-delete-acl"};
            kafkaClusterManager.run(args);
            verify(adminClient, never()).createTopics(any());
            verify(adminClient, never()).createPartitions(any());
            verify(adminClient, never()).createAcls(any());
            verify(adminClient, never()).deleteAcls(any());
            verify(adminClient, never()).incrementalAlterConfigs(any());
            verify(adminClient, never()).deleteTopics(any());
        }

        @Test
        @DisplayName("with the consumer acl being removed when domain no longer exists in topology")
        void testDeleteConsumerAcl() throws InterruptedException, ExecutionException {
            final String topicNameUserUpdated = "de.volkerfaas.arc.public.user_updated";
            final String topicNameTestCreated = "de.volkerfaas.test.public.test_created";

            final Set<AclBinding> aclBindings = createDomainAclBindings("de.volkerfaas.arc.", "User:129849");
            aclBindings.addAll(createDomainAclBindings("de.volkerfaas.test.", "User:138166"));
            aclBindings.addAll(createConsumerAclBindings("de.volkerfaas.arc.public.", "User:138166"));

            final Set<AclBinding> consumerAclBindings = createConsumerAclBindings("de.volkerfaas.test.public.", "User:129849");
            aclBindings.addAll(consumerAclBindings);

            final Map<String, TopicDescription> topicDescriptions = new HashMap<>();
            topicDescriptions.putAll(createTopicPartitionInfos(topicNameUserUpdated, 5, 9));
            topicDescriptions.putAll(createTopicPartitionInfos(topicNameTestCreated, 3, 9));

            final Map<ConfigResource, Config> configs = new HashMap<>();
            configs.putAll(createConfig(topicNameUserUpdated, Set.of(
                    ConfigEntryUtils.createDynamicTopicConfigEntry("cleanup.policy", "compact"),
                    ConfigEntryUtils.createDynamicTopicConfigEntry("min.compaction.lag.ms", "100")
            )));
            configs.putAll(createConfig(topicNameTestCreated, Collections.emptySet()));

            mockDescribeAcls(adminClient, aclBindings);
            mockDescribeCluster(adminClient, "lkc-p5zy2");
            mockDescribeConfigs(adminClient, configs);
            mockDescribeTopics(adminClient, topicDescriptions);
            mockListTopics(adminClient, Set.of(topicNameUserUpdated, topicNameTestCreated));
            mockParseSchema(schemaRegistryClient, "{ \"type\": \"string\" }");
            mockListOffsets(adminClient, Collections.emptyMap());
            mockListConsumerGroups(adminClient, Collections.emptyList());
            mockDescribeConsumerGroups(adminClient, Collections.emptyMap());

            final Set<AclBindingFilter> expectedAclBindingFilters = consumerAclBindings.stream().map(AclBinding::toFilter).collect(Collectors.toSet());
            mockDeleteAcls(adminClient, expectedAclBindingFilters);

            String[] args = new String[]{"--cluster=test", "--directory=" + topologyDirectory, "--allow-delete-acl"};
            kafkaClusterManager.run(args);
            verify(adminClient, never()).createTopics(any());
            verify(adminClient, never()).createPartitions(any());
            verify(adminClient, never()).createAcls(any());
            verify(adminClient, never()).incrementalAlterConfigs(any());
            verify(adminClient, never()).deleteTopics(any());
        }

        @Test
        @DisplayName("with the domain acl being removed when domain no longer exists in topology")
        void testDeleteDomainAcl() throws InterruptedException, ExecutionException {
            final String topicNameUserUpdated = "de.volkerfaas.arc.public.user_updated";
            final String topicNameTestCreated = "de.volkerfaas.test.public.test_created";
            final String topicNameSoundPlayed = "de.volkerfaas.music.public.sound_played";

            final Set<AclBinding> aclBindings = createDomainAclBindings("de.volkerfaas.arc.", "User:129849");
            aclBindings.addAll(createDomainAclBindings("de.volkerfaas.test.", "User:138166"));
            aclBindings.addAll(createConsumerAclBindings("de.volkerfaas.arc.public.", "User:138166"));

            final Set<AclBinding> domainAclBindings = createDomainAclBindings("de.volkerfaas.music.", "User:121739");
            aclBindings.addAll(domainAclBindings);

            final Map<String, TopicDescription> topicDescriptions = new HashMap<>();
            topicDescriptions.putAll(createTopicPartitionInfos(topicNameUserUpdated, 5, 9));
            topicDescriptions.putAll(createTopicPartitionInfos(topicNameTestCreated, 3, 9));
            topicDescriptions.putAll(createTopicPartitionInfos(topicNameSoundPlayed, 20, 9));

            final Map<ConfigResource, Config> configs = new HashMap<>();
            configs.putAll(createConfig(topicNameUserUpdated, Set.of(
                    ConfigEntryUtils.createDynamicTopicConfigEntry("cleanup.policy", "compact"),
                    ConfigEntryUtils.createDynamicTopicConfigEntry("min.compaction.lag.ms", "100")
            )));
            configs.putAll(createConfig(topicNameTestCreated, Collections.emptySet()));
            configs.putAll(createConfig(topicNameSoundPlayed, Collections.emptySet()));

            mockDescribeAcls(adminClient, aclBindings);
            mockDescribeCluster(adminClient, "lkc-p5zy2");
            mockDescribeConfigs(adminClient, configs);
            mockDescribeTopics(adminClient, topicDescriptions);
            mockListTopics(adminClient, Set.of(topicNameUserUpdated, topicNameTestCreated, topicNameSoundPlayed));
            mockParseSchema(schemaRegistryClient, "{ \"type\": \"string\" }");
            mockListOffsets(adminClient, Collections.emptyMap());
            mockListConsumerGroups(adminClient, Collections.emptyList());
            mockDescribeConsumerGroups(adminClient, Collections.emptyMap());

            final Set<AclBindingFilter> expectedAclBindingFilters = domainAclBindings.stream().map(AclBinding::toFilter).collect(Collectors.toSet());
            mockDeleteAcls(adminClient, expectedAclBindingFilters);

            String[] args = new String[]{"--cluster=test", "--directory=" + topologyDirectory, "--allow-delete-acl"};
            kafkaClusterManager.run(args);
            verify(adminClient, never()).createTopics(any());
            verify(adminClient, never()).createPartitions(any());
            verify(adminClient, never()).createAcls(any());
            verify(adminClient, never()).incrementalAlterConfigs(any());
            verify(adminClient, never()).deleteTopics(any());
        }

        @Test
        @DisplayName("with the domain acl not being removed when domain is set")
        void testDeleteDomainAclWithDomain() throws Exception {
            final String topicNameUserUpdated = "de.volkerfaas.arc.public.user_updated";
            final String topicNameTestCreated = "de.volkerfaas.test.public.test_created";
            final String topicNameSoundPlayed = "de.volkerfaas.music.public.sound_played";

            final Set<AclBinding> aclBindings = createDomainAclBindings("de.volkerfaas.arc.", "User:129849");
            aclBindings.addAll(createDomainAclBindings("de.volkerfaas.test.", "User:138166"));
            aclBindings.addAll(createConsumerAclBindings("de.volkerfaas.arc.public.", "User:138166"));

            final Set<AclBinding> domainAclBindings = createDomainAclBindings("de.volkerfaas.music.", "User:121739");
            aclBindings.addAll(domainAclBindings);

            final Map<String, TopicDescription> topicDescriptions = new HashMap<>();
            topicDescriptions.putAll(createTopicPartitionInfos(topicNameUserUpdated, 5, 9));
            topicDescriptions.putAll(createTopicPartitionInfos(topicNameTestCreated, 3, 9));
            topicDescriptions.putAll(createTopicPartitionInfos(topicNameSoundPlayed, 20, 9));

            final Map<ConfigResource, Config> configs = new HashMap<>();
            configs.putAll(createConfig(topicNameUserUpdated, Set.of(
                    ConfigEntryUtils.createDynamicTopicConfigEntry("cleanup.policy", "compact"),
                    ConfigEntryUtils.createDynamicTopicConfigEntry("min.compaction.lag.ms", "100")
            )));
            configs.putAll(createConfig(topicNameTestCreated, Collections.emptySet()));
            configs.putAll(createConfig(topicNameSoundPlayed, Collections.emptySet()));

            mockDescribeAcls(adminClient, aclBindings);
            mockDescribeCluster(adminClient, "lkc-p5zy2");
            mockDescribeConfigs(adminClient, configs);
            mockDescribeTopics(adminClient, topicDescriptions);
            mockListTopics(adminClient, Set.of(topicNameUserUpdated, topicNameTestCreated, topicNameSoundPlayed));
            mockParseSchema(schemaRegistryClient, "{ \"type\": \"string\" }");
            mockTestCompatibility(schemaRegistryClient);
            mockListOffsets(adminClient, Collections.emptyMap());
            mockListConsumerGroups(adminClient, Collections.emptyList());
            mockDescribeConsumerGroups(adminClient, Collections.emptyMap());

            String[] args = new String[]{"deploy", "--cluster=test", "--directory=" + topologyDirectory, "--allow-delete-acl", "--domain=de.volkerfaas.test"};
            final int exitCode = catchSystemExit(() -> kafkaClusterManager.run(args));
            assertEquals(3, exitCode);
            verify(adminClient, never()).createTopics(any());
            verify(adminClient, never()).createPartitions(any());
            verify(adminClient, never()).createAcls(any());
            verify(adminClient, never()).deleteAcls(any());
            verify(adminClient, never()).incrementalAlterConfigs(any());
            verify(adminClient, never()).deleteTopics(any());
        }

    }

    @Nested
    @DisplayName("should build and delete topics from the topology")
    class BuildTopologyWithAllowDeleteTopics {

        @Test
        @DisplayName("without changes to cluster when topology equals cluster")
        void testTopologyEqualsCluster() throws InterruptedException, ExecutionException {
            final String topicNameUserUpdated = "de.volkerfaas.arc.public.user_updated";
            final String topicNameTestCreated = "de.volkerfaas.test.public.test_created";

            final Set<AclBinding> aclBindings = createDomainAclBindings("de.volkerfaas.arc.", "User:129849");
            aclBindings.addAll(createConsumerAclBindings("de.volkerfaas.arc.public.", "User:138166"));
            aclBindings.addAll(createDomainAclBindings("de.volkerfaas.test.", "User:138166"));

            final Map<String, TopicDescription> topicDescriptions = new HashMap<>();
            topicDescriptions.putAll(createTopicPartitionInfos(topicNameUserUpdated, 5, 9));
            topicDescriptions.putAll(createTopicPartitionInfos(topicNameTestCreated, 3, 9));

            final Map<ConfigResource, Config> configs = new HashMap<>();
            configs.putAll(createConfig(topicNameUserUpdated, Set.of(
                    ConfigEntryUtils.createDynamicTopicConfigEntry("cleanup.policy", "compact"),
                    ConfigEntryUtils.createDynamicTopicConfigEntry("min.compaction.lag.ms", "100")
            )));
            configs.putAll(createConfig(topicNameTestCreated, Collections.emptySet()));

            mockDescribeAcls(adminClient, aclBindings);
            mockDescribeCluster(adminClient, "lkc-p5zy2");
            mockDescribeConfigs(adminClient, configs);
            mockDescribeTopics(adminClient, topicDescriptions);
            mockListTopics(adminClient, Set.of(topicNameUserUpdated, topicNameTestCreated));
            mockParseSchema(schemaRegistryClient, "{ \"type\": \"string\" }");
            mockListOffsets(adminClient, Collections.emptyMap());
            mockListConsumerGroups(adminClient, Collections.emptyList());
            mockDescribeConsumerGroups(adminClient, Collections.emptyMap());

            String[] args = new String[]{"--cluster=test", "--directory=" + topologyDirectory, "--allow-delete-topics"};
            kafkaClusterManager.run(args);
            verify(adminClient, never()).createTopics(any());
            verify(adminClient, never()).createPartitions(any());
            verify(adminClient, never()).createAcls(any());
            verify(adminClient, never()).deleteAcls(any());
            verify(adminClient, never()).incrementalAlterConfigs(any());
            verify(adminClient, never()).deleteTopics(any());
        }

        @Test
        @DisplayName("with the topic being removed when topic no longer exists in topology")
        void testDeleteTopic() throws InterruptedException, ExecutionException {
            final String topicNameUserUpdated = "de.volkerfaas.arc.public.user_updated";
            final String topicNameTestCreated = "de.volkerfaas.test.public.test_created";
            final String topicNameSoundPlayed = "de.volkerfaas.music.public.sound_played";

            final Set<AclBinding> aclBindings = createDomainAclBindings("de.volkerfaas.arc.", "User:129849");
            aclBindings.addAll(createDomainAclBindings("de.volkerfaas.test.", "User:138166"));
            aclBindings.addAll(createConsumerAclBindings("de.volkerfaas.arc.public.", "User:138166"));
            aclBindings.addAll(createDomainAclBindings("de.volkerfaas.music.", "User:142637"));

            final Map<String, TopicDescription> topicDescriptions = new HashMap<>();
            topicDescriptions.putAll(createTopicPartitionInfos(topicNameUserUpdated, 5, 9));
            topicDescriptions.putAll(createTopicPartitionInfos(topicNameTestCreated, 3, 9));
            topicDescriptions.putAll(createTopicPartitionInfos(topicNameSoundPlayed, 9, 9));

            final Map<ConfigResource, Config> configs = new HashMap<>();
            configs.putAll(createConfig(topicNameUserUpdated, Set.of(
                    ConfigEntryUtils.createDynamicTopicConfigEntry("cleanup.policy", "compact"),
                    ConfigEntryUtils.createDynamicTopicConfigEntry("min.compaction.lag.ms", "100")
            )));
            configs.putAll(createConfig(topicNameTestCreated, Collections.emptySet()));
            configs.putAll(createConfig(topicNameSoundPlayed, Collections.emptySet()));

            mockDescribeAcls(adminClient, aclBindings);
            mockDescribeCluster(adminClient, "lkc-p5zy2");
            mockDescribeConfigs(adminClient, configs);
            mockDescribeTopics(adminClient, topicDescriptions);
            mockListTopics(adminClient, Set.of(topicNameUserUpdated, topicNameTestCreated, topicNameSoundPlayed));
            mockParseSchema(schemaRegistryClient, "{ \"type\": \"string\" }");
            mockListOffsets(adminClient, Collections.emptyMap());
            mockListConsumerGroups(adminClient, Collections.emptyList());
            mockDescribeConsumerGroups(adminClient, Collections.emptyMap());
            mockDeleteTopics(adminClient, Set.of(topicNameSoundPlayed));

            String[] args = new String[]{"--cluster=test", "--directory=" + topologyDirectory, "--allow-delete-topics"};
            kafkaClusterManager.run(args);
            verify(adminClient, never()).createTopics(any());
            verify(adminClient, never()).createPartitions(any());
            verify(adminClient, never()).createAcls(any());
            verify(adminClient, never()).deleteAcls(any());
            verify(adminClient, never()).incrementalAlterConfigs(any());
        }

        @Test
        @DisplayName("with the topic not being removed when domain is set")
        void testDeleteTopicWithDomain() throws Exception {
            final String topicNameUserUpdated = "de.volkerfaas.arc.public.user_updated";
            final String topicNameTestCreated = "de.volkerfaas.test.public.test_created";
            final String topicNameSoundPlayed = "de.volkerfaas.music.public.sound_played";

            final Set<AclBinding> aclBindings = createDomainAclBindings("de.volkerfaas.arc.", "User:129849");
            aclBindings.addAll(createDomainAclBindings("de.volkerfaas.test.", "User:138166"));
            aclBindings.addAll(createConsumerAclBindings("de.volkerfaas.arc.public.", "User:138166"));
            aclBindings.addAll(createDomainAclBindings("de.volkerfaas.music.", "User:142637"));

            final Map<String, TopicDescription> topicDescriptions = new HashMap<>();
            topicDescriptions.putAll(createTopicPartitionInfos(topicNameUserUpdated, 5, 9));
            topicDescriptions.putAll(createTopicPartitionInfos(topicNameTestCreated, 3, 9));
            topicDescriptions.putAll(createTopicPartitionInfos(topicNameSoundPlayed, 9, 9));

            final Map<ConfigResource, Config> configs = new HashMap<>();
            configs.putAll(createConfig(topicNameUserUpdated, Set.of(
                    ConfigEntryUtils.createDynamicTopicConfigEntry("cleanup.policy", "compact"),
                    ConfigEntryUtils.createDynamicTopicConfigEntry("min.compaction.lag.ms", "100")
            )));
            configs.putAll(createConfig(topicNameTestCreated, Collections.emptySet()));
            configs.putAll(createConfig(topicNameSoundPlayed, Collections.emptySet()));

            mockDescribeAcls(adminClient, aclBindings);
            mockDescribeCluster(adminClient, "lkc-p5zy2");
            mockDescribeConfigs(adminClient, configs);
            mockDescribeTopics(adminClient, topicDescriptions);
            mockListTopics(adminClient, Set.of(topicNameUserUpdated, topicNameTestCreated, topicNameSoundPlayed));
            mockParseSchema(schemaRegistryClient, "{ \"type\": \"string\" }");
            mockTestCompatibility(schemaRegistryClient);
            mockListOffsets(adminClient, Collections.emptyMap());
            mockListConsumerGroups(adminClient, Collections.emptyList());
            mockDescribeConsumerGroups(adminClient, Collections.emptyMap());

            String[] args = new String[]{"deploy", "--cluster=test", "--directory=" + topologyDirectory, "--allow-delete-topics", "--domain=de.volkerfaas.test"};
            final int exitCode = catchSystemExit(() -> kafkaClusterManager.run(args));
            assertEquals(3, exitCode);
            verify(adminClient, never()).createTopics(any());
            verify(adminClient, never()).createPartitions(any());
            verify(adminClient, never()).createAcls(any());
            verify(adminClient, never()).deleteAcls(any());
            verify(adminClient, never()).incrementalAlterConfigs(any());
            verify(adminClient, never()).deleteTopics(any());
        }

    }

    private void verifyConfig(String topicName, InvocationOnMock invocation) {
        final Map<ConfigResource, Collection<AlterConfigOp>> configs = invocation.getArgument(0);
        assertNotNull(configs);
        assertEquals(1, configs.size());
        final ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        assertThat(configs, hasEntryWithIterableValue(is(resource), containsInAnyOrder(
                new AlterConfigOp(new ConfigEntry("cleanup.policy", "compact"), AlterConfigOp.OpType.SET),
                new AlterConfigOp(new ConfigEntry("min.compaction.lag.ms", "100"), AlterConfigOp.OpType.SET)
        )));
    }

}
