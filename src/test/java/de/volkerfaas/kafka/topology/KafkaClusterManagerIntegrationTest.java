package de.volkerfaas.kafka.topology;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.volkerfaas.kafka.topology.model.*;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.invocation.InvocationOnMock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Bean;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SpringBootTest(properties = {
        "cluster=test",
        "documentation.filename=topology.md"
})
@ContextConfiguration(loader= AnnotationConfigContextLoader.class)
@ExtendWith(SpringExtension.class)
@DisplayName("The Kafka Cluster Manager")
public class KafkaClusterManagerIntegrationTest {

    @Autowired
    private CacheManager cacheManager;

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

    @BeforeAll()
    static void setup() {
        System.setProperty("BOOTSTRAP_SERVER", "localhost:9092");
        System.setProperty("CLUSTER_API_KEY", "test_api_key");
        System.setProperty("CLUSTER_API_SECRET", "test_api_secret");
        System.setProperty("SCHEMA_REGISTRY_URL", "http://localhost:8080");
        System.setProperty("SCHEMA_REGISTRY_API_KEY", "test_api_key");
        System.setProperty("SCHEMA_REGISTRY_API_SECRET", "test_api_secret");
    }

    @BeforeEach
    void init() {
        final URL resource = getClass().getClassLoader().getResource("topology-de.volkerfaas.arc.yaml");
        assertNotNull(resource);
        this.topologyDirectory = new File(resource.getPath()).getParent();
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

            String[] args = new String[]{"--directory=" + topologyDirectory};
            ApplicationArguments applicationArguments = new DefaultApplicationArguments(args);
            kafkaClusterManager.run(applicationArguments);
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

            String[] args = new String[]{"--directory=" + topologyDirectory};
            ApplicationArguments applicationArguments = new DefaultApplicationArguments(args);
            kafkaClusterManager.run(applicationArguments);
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

            String[] args = new String[]{"--directory=" + topologyDirectory};
            ApplicationArguments applicationArguments = new DefaultApplicationArguments(args);
            kafkaClusterManager.run(applicationArguments);
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

            String[] args = new String[]{"--directory=" + topologyDirectory};
            ApplicationArguments applicationArguments = new DefaultApplicationArguments(args);
            kafkaClusterManager.run(applicationArguments);
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

            String[] args = new String[]{"--directory=" + topologyDirectory, "--allow-delete-acl"};
            ApplicationArguments applicationArguments = new DefaultApplicationArguments(args);
            kafkaClusterManager.run(applicationArguments);
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

            String[] args = new String[]{"--directory=" + topologyDirectory, "--allow-delete-acl"};
            ApplicationArguments applicationArguments = new DefaultApplicationArguments(args);
            kafkaClusterManager.run(applicationArguments);
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

            String[] args = new String[]{"--directory=" + topologyDirectory, "--allow-delete-acl"};
            ApplicationArguments applicationArguments = new DefaultApplicationArguments(args);
            kafkaClusterManager.run(applicationArguments);
            verify(adminClient, never()).createTopics(any());
            verify(adminClient, never()).createPartitions(any());
            verify(adminClient, never()).createAcls(any());
            verify(adminClient, never()).incrementalAlterConfigs(any());
            verify(adminClient, never()).deleteTopics(any());
        }

        @Test
        @DisplayName("with the domain acl not being removed when domain is set")
        void testDeleteDomainAclWithDomain() throws InterruptedException, ExecutionException {
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

            String[] args = new String[]{"--directory=" + topologyDirectory, "--allow-delete-acl", "--domain=de.volkerfaas.test"};
            ApplicationArguments applicationArguments = new DefaultApplicationArguments(args);
            kafkaClusterManager.run(applicationArguments);
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

            String[] args = new String[]{"--directory=" + topologyDirectory, "--allow-delete-topics"};
            ApplicationArguments applicationArguments = new DefaultApplicationArguments(args);
            kafkaClusterManager.run(applicationArguments);
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

            String[] args = new String[]{"--directory=" + topologyDirectory, "--allow-delete-topics"};
            ApplicationArguments applicationArguments = new DefaultApplicationArguments(args);
            kafkaClusterManager.run(applicationArguments);
            verify(adminClient, never()).createTopics(any());
            verify(adminClient, never()).createPartitions(any());
            verify(adminClient, never()).createAcls(any());
            verify(adminClient, never()).deleteAcls(any());
            verify(adminClient, never()).incrementalAlterConfigs(any());
        }

        @Test
        @DisplayName("with the topic not being removed when domain is set")
        void testDeleteTopicWithDomain() throws InterruptedException, ExecutionException {
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

            String[] args = new String[]{"--directory=" + topologyDirectory, "--allow-delete-topics", "--domain=de.volkerfaas.test"};
            ApplicationArguments applicationArguments = new DefaultApplicationArguments(args);
            kafkaClusterManager.run(applicationArguments);
            verify(adminClient, never()).createTopics(any());
            verify(adminClient, never()).createPartitions(any());
            verify(adminClient, never()).createAcls(any());
            verify(adminClient, never()).deleteAcls(any());
            verify(adminClient, never()).incrementalAlterConfigs(any());
            verify(adminClient, never()).deleteTopics(any());
        }

    }

    @Nested
    @DisplayName("should restore the topology")
    class RestoreTopology {

        @MockBean
        private ObjectMapper objectMapper;

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
            doReturn("FORWARD_TRANSITIVE").when(schemaRegistryClient).getCompatibility(anyString());

            String[] args = new String[]{"--directory=" + topologyDirectory, "--restore", "--domain=" + domainName};
            ApplicationArguments applicationArguments = new DefaultApplicationArguments(args);
            kafkaClusterManager.run(applicationArguments);
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
            doReturn(schemaMetadata).when(schemaRegistryClient).getLatestSchemaMetadata(anyString());
            doReturn("FORWARD_TRANSITIVE").when(schemaRegistryClient).getCompatibility(anyString());

            String[] args = new String[]{"--directory=" + topologyDirectory, "--restore"};
            ApplicationArguments applicationArguments = new DefaultApplicationArguments(args);
            kafkaClusterManager.run(applicationArguments);
            verify(objectMapper, never()).writeValue(any(File.class), any(TopologyFile.class));
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
