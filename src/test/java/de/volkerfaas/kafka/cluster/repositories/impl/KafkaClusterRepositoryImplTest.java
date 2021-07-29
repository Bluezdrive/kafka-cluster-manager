package de.volkerfaas.kafka.cluster.repositories.impl;

import de.volkerfaas.kafka.cluster.model.ClusterConfiguration;
import de.volkerfaas.kafka.cluster.model.ConsumerGroupConfiguration;
import de.volkerfaas.kafka.cluster.model.TopicConfiguration;
import de.volkerfaas.kafka.cluster.repositories.KafkaClusterRepository;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static de.volkerfaas.kafka.topology.utils.MockUtils.*;
import static de.volkerfaas.kafka.topology.utils.TestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

@DisplayName("In the class KafkaClusterRepositoryImpl")
class KafkaClusterRepositoryImplTest {

    private AdminClient adminClient;
    private KafkaClusterRepository kafkaClusterRepository;

    @BeforeEach
    void init() {
        this.adminClient = mock(AdminClient.class);
        this.kafkaClusterRepository = new KafkaClusterRepositoryImpl(adminClient, null, null);
    }

    @Nested
    @DisplayName("when dry run is given")
    class DryRun {

        @BeforeEach
        void init() {
            kafkaClusterRepository = new KafkaClusterRepositoryImpl(adminClient, null, "");
        }

        @Test
        @DisplayName("no acls should be created")
        void testCreateAccessControlLists() throws ExecutionException, InterruptedException {
            final ResourcePattern resourcePattern = new ResourcePattern(ResourceType.TOPIC, "de.volkerfaas.test.public.", PatternType.PREFIXED);
            final AccessControlEntry accessControlEntry = new AccessControlEntry("User:134567", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW);
            final AclBinding aclBinding = new AclBinding(resourcePattern, accessControlEntry);
            final List<AclBinding> aclBindings = List.of(aclBinding);
            kafkaClusterRepository.createAccessControlLists(aclBindings);
            verify(adminClient, never()).createAcls(eq(aclBindings));
        }

        @Test
        @DisplayName("no partitions should be created")
        void testCreatePartitions() throws ExecutionException, InterruptedException {
            Map<String, NewPartitions> newPartitions = new HashMap<>();
            newPartitions.put("de.volkerfaas.test.public.created", NewPartitions.increaseTo(7));
            kafkaClusterRepository.createPartitions(newPartitions);
            verify(adminClient, never()).createPartitions(eq(newPartitions));
        }

        @Test
        @DisplayName("no topics should be created")
        void testCreateTopics() throws ExecutionException, InterruptedException {
            final NewTopic newTopic = new NewTopic("de.volkerfaas.test.public.created", 6, (short) 3).configs(Collections.emptyMap());
            final List<NewTopic> newTopics = List.of(newTopic);
            kafkaClusterRepository.createTopics(newTopics);
            verify(adminClient, never()).createTopics(eq(newTopics));
        }

        @Test
        @DisplayName("no acls should be deleted")
        void testDeleteAccessControlLists() throws ExecutionException, InterruptedException {
            final ResourcePatternFilter resourcePatternFilter = new ResourcePatternFilter(ResourceType.TOPIC, "de.volkerfaas.test.public.", PatternType.PREFIXED);
            final AccessControlEntryFilter accessControlEntryFilter = new AccessControlEntryFilter("User:134567", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW);
            final AclBindingFilter aclBindingFilter = new AclBindingFilter(resourcePatternFilter, accessControlEntryFilter);
            final List<AclBindingFilter> aclBindingFilters = List.of(aclBindingFilter);
            kafkaClusterRepository.deleteAccessControlLists(aclBindingFilters);
            verify(adminClient, never()).deleteAcls(eq(aclBindingFilters));
        }

        @Test
        @DisplayName("no topics should be deleted")
        void testDeleteTopics() throws ExecutionException, InterruptedException {
            final List<String> topicsNames = List.of("de.volkerfaas.test.public.created");
            kafkaClusterRepository.deleteTopics(topicsNames);
            verify(adminClient, never()).deleteTopics(eq(topicsNames));
        }

        @Test
        @DisplayName("no configs should be altered")
        void testUpdateConfigs() throws ExecutionException, InterruptedException {
            Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
            final ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, "de.volkerfaas.test.public.created");
            final ConfigEntry configEntry = new ConfigEntry("cleanup.policy", "compact");
            final AlterConfigOp alterConfigOp = new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET);
            final List<AlterConfigOp> alterConfigOps = List.of(alterConfigOp);
            configs.put(configResource, alterConfigOps);
            kafkaClusterRepository.updateConfigs(configs);
            verify(adminClient, never()).incrementalAlterConfigs(eq(configs));
        }

    }

    @Nested
    @DisplayName("the method getClusterConfiguration")
    class GetClusterConfiguration {

        @Test
        @DisplayName("should return the cluster id")
        void testGetClusterConfigurationClusterId() throws ExecutionException, InterruptedException {
            final String clusterId = "lkc-p5zy2";
            mockDescribeCluster(adminClient, clusterId);
            mockListTopics(adminClient, Collections.emptySet());
            mockDescribeTopics(adminClient, Collections.emptyMap());
            mockDescribeConfigs(adminClient, Collections.emptyMap());
            mockListOffsets(adminClient, Collections.emptyMap());
            mockDescribeAcls(adminClient, Collections.emptySet());
            mockListConsumerGroups(adminClient, Collections.emptyList());
            mockDescribeConsumerGroups(adminClient, Collections.emptyMap());

            final ClusterConfiguration clusterConfiguration = kafkaClusterRepository.getClusterConfiguration();
            assertNotNull(clusterConfiguration);
            assertEquals(clusterId, clusterConfiguration.getClusterId());
        }

        @Test
        @DisplayName("should return the topics")
        void testGetClusterConfigurationTopics() throws ExecutionException, InterruptedException {
            final String clusterId = "lkc-p5zy2";
            final String topicName = "de.volkerfaas.test.public.user_updated";
            final Set<String> topicNames = Set.of(topicName);

            mockDescribeCluster(adminClient, clusterId);
            mockListTopics(adminClient, topicNames);
            mockDescribeTopics(adminClient, createTopicPartitionInfos(topicName, 4, 9));
            mockDescribeConfigs(adminClient, createConfig(topicName, Set.of(
                    ConfigEntryUtils.createDynamicTopicConfigEntry("cleanup.policy", "compact"),
                    ConfigEntryUtils.createDynamicTopicConfigEntry("min.compaction.lag.ms", "100")
            )));
            mockListOffsets(adminClient, Collections.emptyMap());
            mockDescribeAcls(adminClient, Collections.emptySet());
            mockListConsumerGroups(adminClient, Collections.emptyList());
            mockDescribeConsumerGroups(adminClient, Collections.emptyMap());

            final ClusterConfiguration clusterConfiguration = kafkaClusterRepository.getClusterConfiguration();
            assertNotNull(clusterConfiguration);

            final Collection<TopicConfiguration> topics = clusterConfiguration.getTopics();
            assertNotNull(topics);
            assertEquals(1, topics.size());
        }

        @Test
        @DisplayName("should return the consumer groups")
        void testGetClusterConfigurationConsumerGroups() throws ExecutionException, InterruptedException {
            final String topicName = "de.volkerfaas.test.public.user_updated";
            final Set<String> topicNames = Set.of(topicName);

            mockListOffsets(adminClient, Collections.emptyMap());
            mockDescribeAcls(adminClient, Collections.emptySet());
            mockListTopics(adminClient, topicNames);
            mockDescribeCluster(adminClient, "lkc-p5zy2");

            final String groupId = "de.volkerfaas.test.my-service";
            final Collection<ConsumerGroupListing> consumerGroupListings = new HashSet<>();
            consumerGroupListings.add(new ConsumerGroupListing(groupId, true));
            mockListConsumerGroups(adminClient, consumerGroupListings);

            final Map<String, ConsumerGroupDescription> consumerGroupDescriptions = new HashMap<>();
            consumerGroupDescriptions.put(groupId, new ConsumerGroupDescription(groupId, true, Collections.emptyList(), "", ConsumerGroupState.EMPTY, null));
            mockDescribeConsumerGroups(adminClient, consumerGroupDescriptions);

            mockListConsumerGroupOffsets(adminClient, createTopicPartitionOffsetAndMetadata(topicName, 4));
            mockDescribeTopics(adminClient, createTopicPartitionInfos(topicName, 4, 9));
            mockDescribeConfigs(adminClient, createConfig(topicName, Set.of(
                    ConfigEntryUtils.createDynamicTopicConfigEntry("cleanup.policy", "compact"),
                    ConfigEntryUtils.createDynamicTopicConfigEntry("min.compaction.lag.ms", "100")
            )));

            final ClusterConfiguration clusterConfiguration = kafkaClusterRepository.getClusterConfiguration();
            assertNotNull(clusterConfiguration);
            assertEquals("lkc-p5zy2", clusterConfiguration.getClusterId());

            final TopicConfiguration topicConfiguration = clusterConfiguration.getTopics().stream().findFirst().orElse(null);
            assertNotNull(topicConfiguration);
            assertEquals(topicName, topicConfiguration.getName());
            assertEquals(4, topicConfiguration.getPartitions().size());
            assertEquals(9, topicConfiguration.getReplicationFactor());

            final Map<String, String> config = topicConfiguration.getConfig();
            assertNotNull(config);
            assertEquals(2, config.size());

            final String cleanupPolicy = config.get("cleanupPolicy");
            assertNotNull(cleanupPolicy);
            assertEquals("compact", cleanupPolicy);

            final String minCompactionLagMs = config.get("minCompactionLagMs");
            assertNotNull(minCompactionLagMs);
            assertEquals("100", minCompactionLagMs);

            final String consumerGroupId = clusterConfiguration.getConsumerGroups()
                    .stream()
                    .map(ConsumerGroupConfiguration::getGroupId)
                    .findFirst()
                    .orElse(null);
            assertNotNull(consumerGroupId);
            assertEquals(groupId, consumerGroupId);
        }

    }

}
