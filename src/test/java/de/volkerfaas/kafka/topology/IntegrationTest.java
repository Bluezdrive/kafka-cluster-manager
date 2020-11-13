package de.volkerfaas.kafka.topology;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
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
import java.util.stream.IntStream;

import static de.volkerfaas.kafka.topology.utils.Matchers.hasEntryWithIterableValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SpringBootTest
@ContextConfiguration(loader= AnnotationConfigContextLoader.class)
@ExtendWith(SpringExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class IntegrationTest {

    @TestConfiguration
    public static class ContextConfiguration {

        @Bean
        public CacheManager cacheManager() {
            return new ConcurrentMapCacheManager("cluster");
        }

    }

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

    @BeforeEach
    void init() {
        final URL resource = getClass().getClassLoader().getResource("topology-de.volkerfaas.arc.yaml");
        assertNotNull(resource);
        File topologyFile = new File(resource.getPath());
        this.topologyDirectory = topologyFile.getParent();
    }

    @AfterEach
    void destroy() {
        Objects.requireNonNull(cacheManager.getCache("cluster")).clear();
        Mockito.reset(adminClient, schemaRegistryClient);
    }

    @Nested
    class buildTopology {

        @BeforeEach
        void init() {
            Properties properties = new Properties();
            properties.put(ApplicationConfiguration.PROPERTY_KEY_TOPOLOGY_DIRECTORY, topologyDirectory);
            System.setProperty(ApplicationConfiguration.PROPERTY_KEY_TOPOLOGY_DIRECTORY, topologyDirectory);
            properties.put(ApplicationConfiguration.PROPERTY_KEY_TOPOLOGY_ALLOW_DELETE, false);
            properties.put(ApplicationConfiguration.PROPERTY_KEY_TOPOLOGY_DRY_RUN, false);
            PropertiesPropertySource propertySource = new PropertiesPropertySource("confluent-cloud-topology-builder", properties);
            environment.getPropertySources().addFirst(propertySource);
        }

        @Test
        void with_no_changes_to_cluster() throws InterruptedException, ExecutionException, IOException {
            final String topicNameUserUpdated = "de.volkerfaas.arc.public.user_updated";
            final String topicNameTestCreated = "de.volkerfaas.test.public.test_created";
            final Set<String> topicNames = Set.of(topicNameUserUpdated, topicNameTestCreated);
            final Set<AclBinding> aclBindings = getDomainAclBindings("de.volkerfaas.arc.", "User:129849");
            aclBindings.addAll(getDomainAclBindings("de.volkerfaas.test.", "User:138166"));
            aclBindings.addAll(getConsumerAclBindings("de.volkerfaas.arc.public.", "User:138166"));
            final Map<String, TopicDescription> topicDescriptions = new HashMap<>();
            topicDescriptions.putAll(getTopicDescription(topicNameUserUpdated, 5));
            topicDescriptions.putAll(getTopicDescription(topicNameTestCreated, 3));
            final Map<ConfigResource, Config> configs = new HashMap<>();
            configs.putAll(getConfig(topicNameUserUpdated, Map.entry("cleanup.policy", "compact"), Map.entry("min.compaction.lag.ms", "100")));
            configs.putAll(getConfig(topicNameTestCreated));
            final String schemaString = "{ \"type\": \"string\" }";

            mockDescribeAcls(aclBindings);
            mockDescribeCluster();
            mockDescribeConfigs(configs);
            mockDescribeTopics(topicNames, topicDescriptions);
            mockListTopics(topicNames);
            mockParseSchema(schemaString);

            kafkaClusterManager.buildTopology(topologyDirectory, Collections.emptyList());
            verify(adminClient, never()).createTopics(any());
            verify(adminClient, never()).createPartitions(any());
            verify(adminClient, never()).createAcls(any());
            verify(adminClient, never()).deleteAcls(any());
            verify(adminClient, never()).incrementalAlterConfigs(any());
        }

        @Test
        void with_new_partition_created_in_cluster() throws InterruptedException, ExecutionException, IOException {
            final String topicNameUserUpdated = "de.volkerfaas.arc.public.user_updated";
            final String topicNameTestCreated = "de.volkerfaas.test.public.test_created";
            final Set<String> topicNames = Set.of(topicNameUserUpdated, topicNameTestCreated);
            final Set<AclBinding> aclBindings = getDomainAclBindings("de.volkerfaas.arc.", "User:129849");
            aclBindings.addAll(getDomainAclBindings("de.volkerfaas.test.", "User:138166"));
            aclBindings.addAll(getConsumerAclBindings("de.volkerfaas.arc.public.", "User:138166"));
            final Map<String, TopicDescription> topicDescriptions = new HashMap<>();
            topicDescriptions.putAll(getTopicDescription(topicNameUserUpdated, 4));
            topicDescriptions.putAll(getTopicDescription(topicNameTestCreated, 3));
            final Map<ConfigResource, Config> topicConfigs = new HashMap<>();
            topicConfigs.putAll(getConfig(topicNameUserUpdated, Map.entry("cleanup.policy", "compact"), Map.entry("min.compaction.lag.ms", "100")));
            topicConfigs.putAll(getConfig(topicNameTestCreated));
            final String schemaString = "{ \"type\": \"string\" }";

            mockDescribeAcls(aclBindings);
            mockDescribeCluster();
            mockDescribeConfigs(topicConfigs);
            mockDescribeTopics(topicNames, topicDescriptions);
            mockListTopics(topicNames);
            mockParseSchema(schemaString);

            final KafkaFuture<Void> kafkaFuture = mock(KafkaFuture.class);
            doReturn(null).when(kafkaFuture).get();
            final CreatePartitionsResult createPartitionsResult = mock(CreatePartitionsResult.class);
            doReturn(kafkaFuture).when(createPartitionsResult).all();
            doAnswer(invocation -> {
                Map<String, NewPartitions> newPartitions = invocation.getArgument(0);
                assertNotNull(newPartitions);
                assertEquals(1, newPartitions.size());
                assertTrue(newPartitions.containsKey(topicNameUserUpdated));
                assertNotNull(newPartitions.get(topicNameUserUpdated));

                return createPartitionsResult;
            }).when(adminClient).createPartitions(anyMap());

            kafkaClusterManager.buildTopology(topologyDirectory, Collections.emptyList());
            verify(adminClient, never()).createTopics(any());
            verify(adminClient, never()).createAcls(any());
            verify(adminClient, never()).deleteAcls(any());
            verify(adminClient, never()).incrementalAlterConfigs(any());
        }

        @Test
        void with_new_topic_created() throws ExecutionException, InterruptedException, IOException {
            final String topicNameUserUpdated = "de.volkerfaas.arc.public.user_updated";
            final String topicNameTestCreated = "de.volkerfaas.test.public.test_created";
            final int numPartitionsTestCreated = 3;
            final short replicationFactorTestCreated = 3;
            final Set<String> topicNames = Set.of(topicNameUserUpdated);
            final Set<AclBinding> aclBindings = getDomainAclBindings("de.volkerfaas.arc.", "User:129849");
            final Map<String, TopicDescription> topicDescriptions = new HashMap<>();
            topicDescriptions.putAll(getTopicDescription(topicNameUserUpdated, 5));
            final Map<ConfigResource, Config> topicConfigs = new HashMap<>();
            topicConfigs.putAll(getConfig(topicNameUserUpdated, Map.entry("cleanup.policy", "compact"), Map.entry("min.compaction.lag.ms", "100")));
            final String schemaString = "{ \"type\": \"string\" }";

            mockDescribeAcls(aclBindings);
            mockDescribeCluster();
            mockDescribeConfigs(topicConfigs);
            mockDescribeTopics(topicNames, topicDescriptions);
            mockListTopics(topicNames);
            mockParseSchema(schemaString);

            final KafkaFuture<Void> kafkaFuture = mock(KafkaFuture.class);
            doReturn(null).when(kafkaFuture).get();

            final CreateAclsResult createAclsResult = mock(CreateAclsResult.class);
            doReturn(kafkaFuture).when(createAclsResult).all();
            final Set<AclBinding> expectedAclBindings = getDomainAclBindings("de.volkerfaas.test.", "User:138166");
            expectedAclBindings.addAll(getConsumerAclBindings("de.volkerfaas.arc.public.", "User:138166"));
            doAnswer(invocation -> {
                Set<AclBinding> acls = invocation.getArgument(0);
                assertNotNull(acls);
                assertEquals(9, acls.size());
                assertThat(acls, containsInAnyOrder(expectedAclBindings.toArray()));

                return createAclsResult;
            }).when(adminClient).createAcls(anyCollection());

            final CreateTopicsResult createTopicsResult = mock(CreateTopicsResult.class);
            doReturn(kafkaFuture).when(createTopicsResult).all();
            doAnswer(invocation -> {
                Set<NewTopic> newTopics = invocation.getArgument(0);
                assertNotNull(newTopics);
                assertEquals(1, newTopics.size());
                assertThat(newTopics, containsInAnyOrder(
                        new NewTopic(topicNameTestCreated, numPartitionsTestCreated, replicationFactorTestCreated).configs(new HashMap<>())
                ));

                return createTopicsResult;
            }).when(adminClient).createTopics(anySet());

            kafkaClusterManager.buildTopology(topologyDirectory, Collections.emptyList());
            verify(adminClient, never()).createPartitions(any());
            verify(adminClient, never()).deleteAcls(any());
            verify(adminClient, never()).incrementalAlterConfigs(any());
        }

        @Test
        void with_topic_config_changed() throws InterruptedException, ExecutionException, IOException {
            final String topicNameUserUpdated = "de.volkerfaas.arc.public.user_updated";
            final String topicNameTestCreated = "de.volkerfaas.test.public.test_created";
            final Set<String> topicNames = Set.of(topicNameUserUpdated, topicNameTestCreated);
            final Set<AclBinding> aclBindings = getDomainAclBindings("de.volkerfaas.arc.", "User:129849");
            aclBindings.addAll(getDomainAclBindings("de.volkerfaas.test.", "User:138166"));
            aclBindings.addAll(getConsumerAclBindings("de.volkerfaas.arc.public.", "User:138166"));
            final Map<String, TopicDescription> topicDescriptions = new HashMap<>();
            topicDescriptions.putAll(getTopicDescription(topicNameUserUpdated, 5));
            topicDescriptions.putAll(getTopicDescription(topicNameTestCreated, 3));
            final Map<ConfigResource, Config> topicConfigs = new HashMap<>();
            topicConfigs.putAll(getConfig(topicNameUserUpdated));
            topicConfigs.putAll(getConfig(topicNameTestCreated));
            final String schemaString = "{ \"type\": \"string\" }";

            mockDescribeAcls(aclBindings);
            mockDescribeCluster();
            mockDescribeConfigs(topicConfigs);
            mockDescribeTopics(topicNames, topicDescriptions);
            mockListTopics(topicNames);
            mockParseSchema(schemaString);

            final KafkaFuture<Void> kafkaFuture = mock(KafkaFuture.class);
            doReturn(null).when(kafkaFuture).get();

            final AlterConfigsResult alterConfigsResult = mock(AlterConfigsResult.class);
            doReturn(kafkaFuture).when(alterConfigsResult).all();
            doAnswer(invocation -> {
                Map<ConfigResource, Collection<AlterConfigOp>> configs = invocation.getArgument(0);
                assertNotNull(configs);
                assertEquals(1, configs.size());
                final ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicNameUserUpdated);
                assertThat(configs, hasEntryWithIterableValue(is(resource), containsInAnyOrder(
                                new AlterConfigOp(new ConfigEntry("cleanup.policy", "compact"), AlterConfigOp.OpType.SET),
                                new AlterConfigOp(new ConfigEntry("min.compaction.lag.ms", "100"), AlterConfigOp.OpType.SET)
                )));
                return alterConfigsResult;
            }).when(adminClient).incrementalAlterConfigs(anyMap());

            kafkaClusterManager.buildTopology(topologyDirectory, Collections.emptyList());
            verify(adminClient, never()).createTopics(any());
            verify(adminClient, never()).createPartitions(any());
            verify(adminClient, never()).createAcls(any());
            verify(adminClient, never()).deleteAcls(any());
        }

    }

    @Nested
    class buildTopologyWithAllowDelete {

        @BeforeEach
        void init() {
            Properties properties = new Properties();
            properties.put(ApplicationConfiguration.PROPERTY_KEY_TOPOLOGY_DIRECTORY, topologyDirectory);
            System.setProperty(ApplicationConfiguration.PROPERTY_KEY_TOPOLOGY_DIRECTORY, topologyDirectory);
            properties.put(ApplicationConfiguration.PROPERTY_KEY_TOPOLOGY_ALLOW_DELETE, true);
            properties.put(ApplicationConfiguration.PROPERTY_KEY_TOPOLOGY_DRY_RUN, false);
            PropertiesPropertySource propertySource = new PropertiesPropertySource("confluent-cloud-topology-builder", properties);
            environment.getPropertySources().addFirst(propertySource);
        }

        @Test
        void and_no_changes_to_cluster() throws InterruptedException, ExecutionException, IOException {
            final String topicNameUserUpdated = "de.volkerfaas.arc.public.user_updated";
            final String topicNameTestCreated = "de.volkerfaas.test.public.test_created";
            final Set<String> topicNames = Set.of(topicNameUserUpdated, topicNameTestCreated);
            final Set<AclBinding> aclBindings = getDomainAclBindings("de.volkerfaas.arc.", "User:129849");
            aclBindings.addAll(getDomainAclBindings("de.volkerfaas.test.", "User:138166"));
            aclBindings.addAll(getConsumerAclBindings("de.volkerfaas.arc.public.", "User:138166"));
            final Map<String, TopicDescription> topicDescriptions = new HashMap<>();
            topicDescriptions.putAll(getTopicDescription(topicNameUserUpdated, 5));
            topicDescriptions.putAll(getTopicDescription(topicNameTestCreated, 3));
            final Map<ConfigResource, Config> configs = new HashMap<>();
            configs.putAll(getConfig(topicNameUserUpdated, Map.entry("cleanup.policy", "compact"), Map.entry("min.compaction.lag.ms", "100")));
            configs.putAll(getConfig(topicNameTestCreated));
            final String schemaString = "{ \"type\": \"string\" }";

            mockDescribeAcls(aclBindings);
            mockDescribeCluster();
            mockDescribeConfigs(configs);
            mockDescribeTopics(topicNames, topicDescriptions);
            mockListTopics(topicNames);
            mockParseSchema(schemaString);

            kafkaClusterManager.buildTopology(topologyDirectory, Collections.emptyList());
            verify(adminClient, never()).createTopics(any());
            verify(adminClient, never()).createPartitions(any());
            verify(adminClient, never()).createAcls(any());
            verify(adminClient, never()).deleteAcls(any());
            verify(adminClient, never()).incrementalAlterConfigs(any());
        }

        @Test
        void and_removing_consumer_acl() throws InterruptedException, ExecutionException, IOException {
            final String topicNameUserUpdated = "de.volkerfaas.arc.public.user_updated";
            final String topicNameTestCreated = "de.volkerfaas.test.public.test_created";
            final Set<String> topicNames = Set.of(topicNameUserUpdated, topicNameTestCreated);
            final Set<AclBinding> aclBindings = getDomainAclBindings("de.volkerfaas.arc.", "User:129849");
            aclBindings.addAll(getDomainAclBindings("de.volkerfaas.test.", "User:138166"));
            final Set<AclBinding> consumerAclBindings = getConsumerAclBindings("de.volkerfaas.test.public.", "User:129849");
            aclBindings.addAll(consumerAclBindings);
            aclBindings.addAll(getConsumerAclBindings("de.volkerfaas.arc.public.", "User:138166"));
            final Map<String, TopicDescription> topicDescriptions = new HashMap<>();
            topicDescriptions.putAll(getTopicDescription(topicNameUserUpdated, 5));
            topicDescriptions.putAll(getTopicDescription(topicNameTestCreated, 3));
            final Map<ConfigResource, Config> configs = new HashMap<>();
            configs.putAll(getConfig(topicNameUserUpdated, Map.entry("cleanup.policy", "compact"), Map.entry("min.compaction.lag.ms", "100")));
            configs.putAll(getConfig(topicNameTestCreated));
            final String schemaString = "{ \"type\": \"string\" }";

            mockDescribeAcls(aclBindings);
            mockDescribeCluster();
            mockDescribeConfigs(configs);
            mockDescribeTopics(topicNames, topicDescriptions);
            mockListTopics(topicNames);
            mockParseSchema(schemaString);

            final Set<AclBindingFilter> expectedAclBindingFilters = consumerAclBindings.stream().map(AclBinding::toFilter).collect(Collectors.toSet());
            mockDeleteAcls(3, expectedAclBindingFilters);

            kafkaClusterManager.buildTopology(topologyDirectory, Collections.emptyList());
            verify(adminClient, never()).createTopics(any());
            verify(adminClient, never()).createPartitions(any());
            verify(adminClient, never()).createAcls(any());
            verify(adminClient, never()).incrementalAlterConfigs(any());
        }

        @Test
        void and_removing_domain_acl() throws InterruptedException, ExecutionException, IOException {
            final String topicNameUserUpdated = "de.volkerfaas.arc.public.user_updated";
            final String topicNameTestCreated = "de.volkerfaas.test.public.test_created";
            final String topicNameSoundPlayed = "de.volkerfaas.music.public.sound_played";
            final Set<String> topicNames = Set.of(topicNameUserUpdated, topicNameTestCreated, topicNameSoundPlayed);
            final Set<AclBinding> aclBindings = getDomainAclBindings("de.volkerfaas.arc.", "User:129849");
            aclBindings.addAll(getDomainAclBindings("de.volkerfaas.test.", "User:138166"));
            aclBindings.addAll(getConsumerAclBindings("de.volkerfaas.arc.public.", "User:138166"));
            final Set<AclBinding> domainAclBindings = getDomainAclBindings("de.volkerfaas.music.", "User:121739");
            aclBindings.addAll(domainAclBindings);
            final Map<String, TopicDescription> topicDescriptions = new HashMap<>();
            topicDescriptions.putAll(getTopicDescription(topicNameUserUpdated, 5));
            topicDescriptions.putAll(getTopicDescription(topicNameTestCreated, 3));
            topicDescriptions.putAll(getTopicDescription(topicNameSoundPlayed, 20));
            final Map<ConfigResource, Config> configs = new HashMap<>();
            configs.putAll(getConfig(topicNameUserUpdated, Map.entry("cleanup.policy", "compact"), Map.entry("min.compaction.lag.ms", "100")));
            configs.putAll(getConfig(topicNameTestCreated));
            configs.putAll(getConfig(topicNameSoundPlayed));
            final String schemaString = "{ \"type\": \"string\" }";

            mockDescribeAcls(aclBindings);
            mockDescribeCluster();
            mockDescribeConfigs(configs);
            mockDescribeTopics(topicNames, topicDescriptions);
            mockListTopics(topicNames);
            mockParseSchema(schemaString);

            final Set<AclBindingFilter> expectedAclBindingFilters = domainAclBindings.stream().map(AclBinding::toFilter).collect(Collectors.toSet());
            mockDeleteAcls(6, expectedAclBindingFilters);

            kafkaClusterManager.buildTopology(topologyDirectory, Collections.emptyList());
            verify(adminClient, never()).createTopics(any());
            verify(adminClient, never()).createPartitions(any());
            verify(adminClient, never()).createAcls(any());
            verify(adminClient, never()).incrementalAlterConfigs(any());
        }

    }

    private void mockDeleteAcls(int expectedSize, Set<AclBindingFilter> expectedAclBindingFilters) throws InterruptedException, ExecutionException {
        final KafkaFuture<Void> kafkaFuture = mock(KafkaFuture.class);
        doReturn(null).when(kafkaFuture).get();
        final DeleteAclsResult deleteAclsResult = mock(DeleteAclsResult.class);
        doReturn(kafkaFuture).when(deleteAclsResult).all();
        doAnswer(invocation -> {
            Collection<AclBindingFilter> filters = invocation.getArgument(0);
            assertNotNull(filters);
            assertEquals(expectedSize, filters.size());
            assertThat(filters, containsInAnyOrder(expectedAclBindingFilters.toArray()));
            return deleteAclsResult;
        }).when(adminClient).deleteAcls(anyCollection());
    }

    private void mockParseSchema(String schemaString) {
        doReturn(Optional.of(new AvroSchema(schemaString))).when(schemaRegistryClient).parseSchema(eq(AvroSchema.TYPE), anyString(), eq(Collections.emptyList()));
    }

    private void mockListTopics(Set<String> topicNames) throws InterruptedException, ExecutionException {
        final KafkaFuture<Set<String>> kafkaFutureNames = mock(KafkaFuture.class);
        final ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
        doReturn(topicNames).when(kafkaFutureNames).get();
        doReturn(kafkaFutureNames).when(listTopicsResult).names();
        doReturn(listTopicsResult).when(adminClient).listTopics();
    }

    private void mockDescribeTopics(Set<String> topicNames, Map<String, TopicDescription> topicDescriptions) throws InterruptedException, ExecutionException {
        final KafkaFuture<Map<String, TopicDescription>> kafkaFutureTopicDescription = mock(KafkaFuture.class);
        final DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
        doReturn(topicDescriptions).when(kafkaFutureTopicDescription).get();
        doReturn(kafkaFutureTopicDescription).when(describeTopicsResult).all();
        doReturn(describeTopicsResult).when(adminClient).describeTopics(eq(topicNames));
    }

    private void mockDescribeConfigs(Map<ConfigResource, Config> configMap) throws InterruptedException, ExecutionException {
        final KafkaFuture<Map<ConfigResource, Config>> kafkaFutureConfig = mock(KafkaFuture.class);
        final DescribeConfigsResult describeConfigsResult = mock(DescribeConfigsResult.class);
        doReturn(configMap).when(kafkaFutureConfig).get();
        doReturn(kafkaFutureConfig).when(describeConfigsResult).all();
        doReturn(describeConfigsResult).when(adminClient).describeConfigs(anySet());
    }

    private void mockDescribeAcls(Set<AclBinding> aclBindings) throws InterruptedException, ExecutionException {
        final KafkaFuture<Set<AclBinding>> kafkaFutureAclBindings = mock(KafkaFuture.class);
        final DescribeAclsResult describeAclsResult = mock(DescribeAclsResult.class);
        doReturn(aclBindings).when(kafkaFutureAclBindings).get();
        doReturn(kafkaFutureAclBindings).when(describeAclsResult).values();
        doReturn(describeAclsResult).when(adminClient).describeAcls(any());
    }

    private void mockDescribeCluster() throws InterruptedException, ExecutionException {
        final KafkaFuture<String> kafkaFutureClusterId = mock(KafkaFuture.class);
        final DescribeClusterResult describeClusterResult = mock(DescribeClusterResult.class);
        doReturn("lkc-p5zy2").when(kafkaFutureClusterId).get();
        doReturn(kafkaFutureClusterId).when(describeClusterResult).clusterId();
        doReturn(describeClusterResult).when(adminClient).describeCluster();
    }

    private Map<ConfigResource, Config> getConfig(String topicName, Map.Entry<String, String>... configEntries) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        Config kafkaConfig = new Config(Arrays.stream(configEntries)
                .map(ConfigEntries::createDynamicTopicConfigEntry)
                .collect(Collectors.toSet()));

        Map<ConfigResource, Config> configs = new HashMap<>();
        configs.put(resource, kafkaConfig);

        return configs;
    }

    private Map<String, TopicDescription> getTopicDescription(String topicName, int numPartitions) {
        final List<Node> replicas = List.of(
                new Node(1, "localhost", 9092),
                new Node(2, "localhost", 9092),
                new Node(3, "localhost", 9092),
                new Node(4, "localhost", 9092),
                new Node(5, "localhost", 9092)
        );
        final List<TopicPartitionInfo> topicPartitionInfos = IntStream.range(0, numPartitions)
                .mapToObj(partition -> new TopicPartitionInfo(0, null, replicas, Collections.emptyList()))
                .collect(Collectors.toList());
        final TopicDescription topicDescription = new TopicDescription(topicName, false, topicPartitionInfos);

        Map<String, TopicDescription> topicDescriptions = new HashMap<>();
        topicDescriptions.put(topicName, topicDescription);

        return topicDescriptions;
    }

    private Set<AclBinding> getDomainAclBindings(String name, String principal) {
        ResourcePattern resourcePatternGroup = new ResourcePattern(ResourceType.GROUP, name, PatternType.PREFIXED);
        ResourcePattern resourcePatternTopic = new ResourcePattern(ResourceType.TOPIC, name, PatternType.PREFIXED);
        ResourcePattern resourcePatternTransactionalId = new ResourcePattern(ResourceType.TRANSACTIONAL_ID, name, PatternType.PREFIXED);
        ResourcePattern resourcePatternCluster = new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL);
        return new HashSet<>(Set.of(
                new AclBinding(resourcePatternGroup, new AccessControlEntry(principal, "*", AclOperation.READ, AclPermissionType.ALLOW)),
                new AclBinding(resourcePatternTopic, new AccessControlEntry(principal, "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)),
                new AclBinding(resourcePatternTopic, new AccessControlEntry(principal, "*", AclOperation.READ, AclPermissionType.ALLOW)),
                new AclBinding(resourcePatternTopic, new AccessControlEntry(principal, "*", AclOperation.WRITE, AclPermissionType.ALLOW)),
                new AclBinding(resourcePatternTransactionalId, new AccessControlEntry(principal, "*", AclOperation.WRITE, AclPermissionType.ALLOW)),
                new AclBinding(resourcePatternCluster, new AccessControlEntry(principal, "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW))
        ));
    }

    private Set<AclBinding> getConsumerAclBindings(String name, String principal) {
        ResourcePattern resourcePatternGroup = new ResourcePattern(ResourceType.GROUP, name, PatternType.PREFIXED);
        ResourcePattern resourcePatternTopic = new ResourcePattern(ResourceType.TOPIC, name, PatternType.PREFIXED);
        return Set.of(
                new AclBinding(resourcePatternGroup, new AccessControlEntry(principal, "*", AclOperation.READ, AclPermissionType.ALLOW)),
                new AclBinding(resourcePatternTopic, new AccessControlEntry(principal, "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)),
                new AclBinding(resourcePatternTopic, new AccessControlEntry(principal, "*", AclOperation.READ, AclPermissionType.ALLOW))
        );
    }

}
