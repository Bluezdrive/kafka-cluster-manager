package de.volkerfaas.kafka.topology;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.invocation.InvocationOnMock;
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

import static de.volkerfaas.kafka.topology.utils.MatcherUtils.hasEntryWithIterableValue;
import static de.volkerfaas.kafka.topology.utils.MockUtils.*;
import static de.volkerfaas.kafka.topology.utils.TestUtils.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SpringBootTest(properties = {"documentation.filename=topology.md"})
@ContextConfiguration(loader= AnnotationConfigContextLoader.class)
@ExtendWith(SpringExtension.class)
@DisplayName("The kafka topology manager")
class BuildTopologyIntegrationTest {

    private static String ENVIRONMENT = "test";

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

    private String topologyDirectory;

    @TestConfiguration
    public static class ContextConfiguration {

        @Bean
        public CacheManager cacheManager() {
            return new ConcurrentMapCacheManager("cluster");
        }

    }

    @BeforeEach
    void init() {
        final URL resource = getClass().getClassLoader().getResource("topology-de.volkerfaas.arc.yaml");
        assertNotNull(resource);
        final File topologyFile = new File(resource.getPath());
        this.topologyDirectory = topologyFile.getParent();
    }

    @AfterEach
    void destroy() {
        Objects.requireNonNull(cacheManager.getCache("cluster")).clear();
        reset(adminClient, schemaRegistryClient);
    }

    @Nested
    @DisplayName("should build the topology")
    class BuildTopology {

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
        @DisplayName("without changes to cluster when topology equals cluster")
        void testTopologyEqualsCluster() throws InterruptedException, ExecutionException, IOException {
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

            kafkaClusterManager.buildTopology(topologyDirectory, ENVIRONMENT, Collections.emptyList(), false, false);
            verify(adminClient, never()).createTopics(any());
            verify(adminClient, never()).createPartitions(any());
            verify(adminClient, never()).createAcls(any());
            verify(adminClient, never()).deleteAcls(any());
            verify(adminClient, never()).incrementalAlterConfigs(any());
        }

        @Test
        @DisplayName("with one new partition being created when partitions in cluster are less than partitions in topology")
        void testNewPartition() throws InterruptedException, ExecutionException, IOException {
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

            kafkaClusterManager.buildTopology(topologyDirectory, ENVIRONMENT, Collections.emptyList(), false, false);
            verify(adminClient, never()).createTopics(any());
            verify(adminClient, never()).createAcls(any());
            verify(adminClient, never()).deleteAcls(any());
            verify(adminClient, never()).incrementalAlterConfigs(any());
        }

        @Test
        @DisplayName("with one new topic being created when it doesn't exist in cluster")
        void testNewTopic() throws ExecutionException, InterruptedException, IOException {
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

            kafkaClusterManager.buildTopology(topologyDirectory, ENVIRONMENT, Collections.emptyList(), false, false);
            verify(adminClient, never()).createPartitions(any());
            verify(adminClient, never()).deleteAcls(any());
            verify(adminClient, never()).incrementalAlterConfigs(any());
        }

        @Test
        @DisplayName("with config items being set when they don't exist in cluster")
        void testConfigChanged() throws InterruptedException, ExecutionException, IOException {
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
                    })
                    .when(adminClient)
                    .incrementalAlterConfigs(anyMap());

            kafkaClusterManager.buildTopology(topologyDirectory, ENVIRONMENT, Collections.emptyList(), false, false);
            verify(adminClient, never()).createTopics(any());
            verify(adminClient, never()).createPartitions(any());
            verify(adminClient, never()).createAcls(any());
            verify(adminClient, never()).deleteAcls(any());
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

    @Nested
    @DisplayName("should build and delete acls from the topology")
    class BuildTopologyWithAllowDeleteAcl {

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
        @DisplayName("without changes to cluster when topology equals cluster")
        void testTopologyEqualsCluster() throws InterruptedException, ExecutionException, IOException {
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

            kafkaClusterManager.buildTopology(topologyDirectory, ENVIRONMENT, Collections.emptyList(), true, false);
            verify(adminClient, never()).createTopics(any());
            verify(adminClient, never()).createPartitions(any());
            verify(adminClient, never()).createAcls(any());
            verify(adminClient, never()).deleteAcls(any());
            verify(adminClient, never()).incrementalAlterConfigs(any());
            verify(adminClient, never()).deleteTopics(any());
        }

        @Test
        @DisplayName("with the consumer acl being removed when domain no longer exists in topology")
        void testDeleteConsumerAcl() throws InterruptedException, ExecutionException, IOException {
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

            kafkaClusterManager.buildTopology(topologyDirectory, ENVIRONMENT, Collections.emptyList(), true, false);
            verify(adminClient, never()).createTopics(any());
            verify(adminClient, never()).createPartitions(any());
            verify(adminClient, never()).createAcls(any());
            verify(adminClient, never()).incrementalAlterConfigs(any());
            verify(adminClient, never()).deleteTopics(any());
        }

        @Test
        @DisplayName("with the domain acl being removed when domain no longer exists in topology")
        void testDeleteDomainAcl() throws InterruptedException, ExecutionException, IOException {
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

            kafkaClusterManager.buildTopology(topologyDirectory, ENVIRONMENT, Collections.emptyList(), true, false);
            verify(adminClient, never()).createTopics(any());
            verify(adminClient, never()).createPartitions(any());
            verify(adminClient, never()).createAcls(any());
            verify(adminClient, never()).incrementalAlterConfigs(any());
            verify(adminClient, never()).deleteTopics(any());

        }

    }

    @Nested
    @DisplayName("should build and delete topics from the topology")
    class BuildTopologyWithAllowDeleteTopics {

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
        @DisplayName("without changes to cluster when topology equals cluster")
        void testTopologyEqualsCluster() throws InterruptedException, ExecutionException, IOException {
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

            kafkaClusterManager.buildTopology(topologyDirectory, ENVIRONMENT, Collections.emptyList(), false, true);
            verify(adminClient, never()).createTopics(any());
            verify(adminClient, never()).createPartitions(any());
            verify(adminClient, never()).createAcls(any());
            verify(adminClient, never()).deleteAcls(any());
            verify(adminClient, never()).incrementalAlterConfigs(any());
            verify(adminClient, never()).deleteTopics(any());
        }

        @Test
        @DisplayName("with the topic being removed when topic no longer exists in topology")
        void testDeleteTopic() throws InterruptedException, ExecutionException, IOException {
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

            kafkaClusterManager.buildTopology(topologyDirectory, ENVIRONMENT, Collections.emptyList(), false, true);
            verify(adminClient, never()).createTopics(any());
            verify(adminClient, never()).createPartitions(any());
            verify(adminClient, never()).createAcls(any());
            verify(adminClient, never()).deleteAcls(any());
            verify(adminClient, never()).incrementalAlterConfigs(any());
        }

    }

}
