package de.volkerfaas.kafka.topology.services.impl;

import de.volkerfaas.kafka.cluster.model.*;
import de.volkerfaas.kafka.cluster.repositories.KafkaClusterRepository;
import de.volkerfaas.kafka.cluster.repositories.impl.KafkaClusterRepositoryImpl;
import de.volkerfaas.kafka.topology.ApplicationConfiguration;
import de.volkerfaas.kafka.topology.model.Domain;
import de.volkerfaas.kafka.topology.model.Schema;
import de.volkerfaas.kafka.topology.model.Topic;
import de.volkerfaas.kafka.topology.model.Visibility;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("In the class TopicServiceImpl")
class TopicServiceImplTest {

    private KafkaClusterRepository kafkaClusterRepository;
    private TopicServiceImpl topicService;

    @BeforeEach
    void init() {
        this.kafkaClusterRepository = mock(KafkaClusterRepositoryImpl.class);
        this.topicService = new TopicServiceImpl(null, kafkaClusterRepository);
    }

    @Nested
    @DisplayName("the method createNewTopics")
    class CreateNewTopics {

        @Test
        @DisplayName("should not create a new topic if it already exists in cluster")
        void testNotCreateNewTopic() throws ExecutionException, InterruptedException {
            final String fullTopicName = "de.volkerfaas.arc.public.user_updated";
            mockTestClusterConfiguration(fullTopicName, 4, Collections.emptyMap());

            final Domain domain = createTestDomain(4, Collections.emptyMap());
            final Set<NewTopic> newTopics = topicService.createNewTopics(List.of(domain));
            assertNotNull(newTopics);
            assertEquals(0, newTopics.size());
        }

        @Test
        @DisplayName("should create a new topic if it doesn't exist in cluster")
        void testCreateNewTopic() throws ExecutionException, InterruptedException {
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("lkc-p5zy2");
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

            final Domain domain = createTestDomain(4, Collections.emptyMap());
            final Set<NewTopic> newTopics = topicService.createNewTopics(List.of(domain));
            assertNotNull(newTopics);
            assertEquals(1, newTopics.size());
            final NewTopic newTopic = newTopics.stream().findFirst().orElse(null);
            assertNotNull(newTopic);
            assertEquals("de.volkerfaas.arc.public.user_updated", newTopic.name());
        }

    }

    @Nested
    @DisplayName("the method createNewPartitions")
    class CreateNewPartitions {

        @Test
        @DisplayName("should not create a new partition if number of partitions in topology is greater than in cluster")
        void testCreateNewPartition() throws ExecutionException, InterruptedException {
            final String fullTopicName = "de.volkerfaas.arc.public.user_updated";
            mockTestClusterConfiguration(fullTopicName, 4, Collections.emptyMap());

            final Domain domain = createTestDomain(5, Collections.emptyMap());
            final Map<String, NewPartitions> newPartitionsByTopic = topicService.createNewPartitions(List.of(domain));
            assertNotNull(newPartitionsByTopic);
            assertEquals(1, newPartitionsByTopic.size());
            assertTrue(newPartitionsByTopic.containsKey(fullTopicName));

            final NewPartitions newPartitions = newPartitionsByTopic.get(fullTopicName);
            assertNotNull(newPartitions);
            assertEquals(5, newPartitions.totalCount());
        }

        @Test
        @DisplayName("should not create a new partition if number of partitions in topology is equal with cluster")
        void testNotCreateNewPartitionWhenEqual() throws ExecutionException, InterruptedException {
            final String fullTopicName = "de.volkerfaas.arc.public.user_updated";
            mockTestClusterConfiguration(fullTopicName, 4, Collections.emptyMap());

            final Domain domain = createTestDomain(4, Collections.emptyMap());
            final Map<String, NewPartitions> newPartitionsByTopic = topicService.createNewPartitions(List.of(domain));
            assertNotNull(newPartitionsByTopic);
            assertEquals(0, newPartitionsByTopic.size());
            assertFalse(newPartitionsByTopic.containsKey("de.volkerfaas.arc.public.user_updated"));
        }

        @Test
        @DisplayName("should not create a new partition if number of partitions in topology is less than cluster")
        void testNotCreateNewPartitionWhenLess() {
            final String fullTopicName = "de.volkerfaas.arc.public.user_updated";
            mockTestClusterConfiguration(fullTopicName, 4, Collections.emptyMap());

            final Domain domain = createTestDomain(3, Collections.emptyMap());
            final Exception exception = assertThrows(Exception.class, () -> topicService.createNewPartitions(List.of(domain)));
            assertNotNull(exception);
            assertEquals(IllegalArgumentException.class, exception.getClass());
            assertEquals("Topic '" + fullTopicName + "' has smaller partitions size than in cluster.", exception.getMessage());
        }

    }

    @Nested
    @DisplayName("the method createNewTopic")
    class CreateNewTopic {

        @Test
        @DisplayName("should create a new topic from the topology specification")
        void testCreateNewTopic() {
            final Topic topic = new Topic();
            topic.setPrefix("de.volkerfaas.arc.public.");
            topic.setName("user_updated");
            topic.setValueSchema(new Schema("de.volkerfaas.arc.public.user_updated-value", Schema.Type.AVRO, Schema.CompatibilityMode.FORWARD_TRANSITIVE));
            topic.getConfig().put(ApplicationConfiguration.TOPIC_CONFIG_KEY_CLEANUP_POLICY, "compact");

            final NewTopic newTopic = topicService.createNewTopic(topic);
            assertEquals("de.volkerfaas.arc.public.user_updated", newTopic.name());
            assertEquals(6, newTopic.numPartitions());
            assertEquals(3, newTopic.replicationFactor());
            assertEquals("compact", newTopic.configs().get(ApplicationConfiguration.TOPIC_CONFIG_KEY_CLEANUP_POLICY));
        }

    }

    @Nested
    @DisplayName("the method createAlterConfigOperations")
    class CreateAlterConfigOperations {

        @Test
        @DisplayName("should create new config entries if not available in cluster")
        void testCreateNewAlterConfigOperations() throws ExecutionException, InterruptedException {
            final String fullTopicName = "de.volkerfaas.arc.public.user_updated";
            mockTestClusterConfiguration(fullTopicName, 4, Collections.emptyMap());

            final Map<String, String> newConfig = new HashMap<>();
            newConfig.put("cleanupPolicy", "compact");
            final Domain domain = createTestDomain(4, newConfig);
            final ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, fullTopicName);
            final Map<ConfigResource, Collection<AlterConfigOp>> configs = topicService.createAlterConfigOperations(List.of(domain));
            assertNotNull(configs);
            assertTrue(configs.containsKey(configResource));
            final Collection<AlterConfigOp> alterConfigOps = configs.get(configResource);
            assertNotNull(alterConfigOps);
            assertEquals(1, alterConfigOps.size());
            final AlterConfigOp alterConfigOp = alterConfigOps.stream().findFirst().orElse(null);
            assertNotNull(alterConfigOp);
            assertEquals(AlterConfigOp.OpType.SET, alterConfigOp.opType());
            final ConfigEntry configEntry = alterConfigOp.configEntry();
            assertNotNull(configEntry);
            assertEquals("cleanup.policy", configEntry.name());
            assertEquals("compact", configEntry.value());
        }

        @Test
        @DisplayName("should update config entry if changed")
        void testUpdateAlterConfigOperations() throws ExecutionException, InterruptedException {
            final String fullTopicName = "de.volkerfaas.arc.public.user_updated";
            final Map<String, String> config = new HashMap<>();
            config.put("cleanupPolicy", "delete");
            mockTestClusterConfiguration(fullTopicName, 4, config);

            final Map<String, String> alteredConfig = new HashMap<>();
            alteredConfig.put("cleanupPolicy", "compact");
            final Domain domain = createTestDomain(4, alteredConfig);
            final ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, fullTopicName);
            final Map<ConfigResource, Collection<AlterConfigOp>> configs = topicService.createAlterConfigOperations(List.of(domain));
            assertNotNull(configs);
            assertTrue(configs.containsKey(configResource));
            final Collection<AlterConfigOp> alterConfigOps = configs.get(configResource);
            assertNotNull(alterConfigOps);
            assertEquals(1, alterConfigOps.size());
            final AlterConfigOp alterConfigOp = alterConfigOps.stream().findFirst().orElse(null);
            assertNotNull(alterConfigOp);
            assertEquals(AlterConfigOp.OpType.SET, alterConfigOp.opType());
            final ConfigEntry configEntry = alterConfigOp.configEntry();
            assertNotNull(configEntry);
            assertEquals("cleanup.policy", configEntry.name());
            assertEquals("compact", configEntry.value());
        }

        @Test
        @DisplayName("should not update config entry if nothing changed")
        void testNotAlterConfigOperationsEqual() throws ExecutionException, InterruptedException {
            final String fullTopicName = "de.volkerfaas.arc.public.user_updated";
            final Map<String, String> config = new HashMap<>();
            config.put("cleanupPolicy", "compact");
            mockTestClusterConfiguration(fullTopicName, 4, config);

            final Domain domain = createTestDomain(4, config);
            final ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, fullTopicName);
            final Map<ConfigResource, Collection<AlterConfigOp>> configs = topicService.createAlterConfigOperations(List.of(domain));
            assertNotNull(configs);
            assertFalse(configs.containsKey(configResource));
        }

        @Test
        @DisplayName("should not update config entry if topic not exists")
        void testNotAlterConfigOperationsTopicMissing() throws ExecutionException, InterruptedException {
            final String fullTopicName = "de.volkerfaas.arc.public.user_updated";
            mockEmptyClusterConfiguration();

            Map<String, String> config = new HashMap<>();
            config.put("cleanupPolicy", "compact");
            final Domain domain = createTestDomain(4, config);
            final ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, fullTopicName);
            final Map<ConfigResource, Collection<AlterConfigOp>> configs = topicService.createAlterConfigOperations(List.of(domain));
            assertNotNull(configs);
            assertFalse(configs.containsKey(configResource));
        }

        @Test
        @DisplayName("should delete config entry if not exists in topology")
        void testDeleteAlterConfigOperations() throws ExecutionException, InterruptedException {
            final String fullTopicName = "de.volkerfaas.arc.public.user_updated";
            final Map<String, String> config = new HashMap<>();
            config.put("cleanupPolicy", "compact");
            mockTestClusterConfiguration(fullTopicName, 4, config);

            final Domain domain = createTestDomain(4, Collections.emptyMap());
            final ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, fullTopicName);
            final Map<ConfigResource, Collection<AlterConfigOp>> configs = topicService.createAlterConfigOperations(List.of(domain));
            assertNotNull(configs);
            assertTrue(configs.containsKey(configResource));
            final Collection<AlterConfigOp> alterConfigOps = configs.get(configResource);
            assertNotNull(alterConfigOps);
            assertEquals(1, alterConfigOps.size());
            final AlterConfigOp alterConfigOp = alterConfigOps.stream().findFirst().orElse(null);
            assertNotNull(alterConfigOp);
            assertEquals(AlterConfigOp.OpType.DELETE, alterConfigOp.opType());
            final ConfigEntry configEntry = alterConfigOp.configEntry();
            assertNotNull(configEntry);
            assertEquals("cleanup.policy", configEntry.name());
            assertEquals("compact", configEntry.value());
        }

    }

    @Nested
    @DisplayName("the method isOrphanedTopic")
    class IsOrphanedTopic {

        @ParameterizedTest
        @ValueSource(strings = {
                "de.volkerfaas.test.public.user_updated",
                "de.volkerfaas.test.private.user_updated_error",
                "de.volkerfaas.test.protected.customer_data_updated"
        })
        @DisplayName("should return active consumer groups")
        void testIsOrphanedTopic(String clusterTopicName) throws ExecutionException, InterruptedException {
            ClusterConfiguration clusterConfiguration = new ClusterConfiguration("lkc-p5zy2");
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

            final boolean result = topicService.isOrphanedTopic(clusterTopicName, Collections.emptySet());
            assertTrue(result);
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "_confluent-ksql-pksqlc-zm39y",
                "pksqlc-zm39y"
        })
        @DisplayName("should return active consumer groups")
        void testIsNotOrphanedTopic(String clusterTopicName) throws ExecutionException, InterruptedException {
            ClusterConfiguration clusterConfiguration = new ClusterConfiguration("lkc-p5zy2");
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

            final boolean result = topicService.isOrphanedTopic(clusterTopicName, Collections.emptySet());
            assertFalse(result);
        }

    }

    @Nested
    @DisplayName("the method isTopicAvailable")
    class IsTopicAvailable {

        @Test
        @DisplayName("should return true if topic exists in list of topic names")
        void testIsTopicAvailable() {
            final Topic topic = new Topic();
            topic.setPrefix("de.volkerfaas.arc.public.");
            topic.setName("user_updated");

            final Set<String> existingTopicNames = Set.of("de.volkerfaas.arc.public.user_updated");
            final boolean topicAvailable = topicService.isTopicAvailable(topic, existingTopicNames);
            assertTrue(topicAvailable);
        }

        @Test
        @DisplayName("should return false if topic doesn't exist in list of topic names")
        void testNotIsTopicAvailable() {
            final Topic topic = new Topic();
            topic.setPrefix("de.volkerfaas.arc.public.");
            topic.setName("card_updated");

            final Set<String> existingTopicNames = Set.of("de.volkerfaas.arc.public.user_updated");
            final boolean topicAvailable = topicService.isTopicAvailable(topic, existingTopicNames);
            assertFalse(topicAvailable);
        }

    }

    @Nested
    @DisplayName("the method listTopicsInCluster")
    class ListTopicsInCluster {

        @Test
        @DisplayName("should return a list of topics available in cluster")
        void testListTopicsInCluster() throws ExecutionException, InterruptedException {
            final String fullTopicName = "de.volkerfaas.arc.public.user_updated";
            ClusterConfiguration clusterConfiguration = mockTestClusterConfiguration(fullTopicName, 4, Collections.emptyMap());
            assertNotNull(clusterConfiguration);
            final TopicConfiguration topicConfiguration = clusterConfiguration.getTopics().stream().findFirst().orElse(null);

            final Collection<TopicConfiguration> topicConfigurations = topicService.listTopicsInCluster();
            assertNotNull(topicConfigurations);
            assertEquals(1, topicConfigurations.size());
            assertTrue(topicConfigurations.contains(topicConfiguration));
        }

        @Test
        @DisplayName("should return an empty list if no topics available in cluster")
        void testEmptyListTopicsInClusterNoTopics() throws ExecutionException, InterruptedException {
            mockEmptyClusterConfiguration();

            final Collection<TopicConfiguration> topicConfigurations = topicService.listTopicsInCluster();
            assertNotNull(topicConfigurations);
            assertEquals(0, topicConfigurations.size());
        }

        @Test
        @DisplayName("should return an empty list if no cluster")
        void testEmptyListTopicsInClusterNoCluster() throws ExecutionException, InterruptedException {
            doReturn(null).when(kafkaClusterRepository).getClusterConfiguration();

            final Collection<TopicConfiguration> topicConfigurations = topicService.listTopicsInCluster();
            assertNotNull(topicConfigurations);
            assertEquals(0, topicConfigurations.size());
        }

    }

    @Nested
    @DisplayName("the method listOrphanedTopics")
    class ListOrphanedTopics {

        @Test
        @DisplayName("should return orphaned topics if topology has less topics than cluster")
        void testListOrphanedTopicsLess() throws ExecutionException, InterruptedException {
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("lkc-p5zy2");
            final String fullTopicNameUserUpdated = "de.volkerfaas.arc.public.user_updated";
            final List<PartitionConfiguration> partitionsUserUpdated = List.of(new PartitionConfiguration(fullTopicNameUserUpdated, 0), new PartitionConfiguration(fullTopicNameUserUpdated, 1), new PartitionConfiguration(fullTopicNameUserUpdated, 2), new PartitionConfiguration(fullTopicNameUserUpdated, 3));
            final TopicConfiguration topicConfigurationUserUpdated = new TopicConfiguration(fullTopicNameUserUpdated, partitionsUserUpdated, (short) 3, Collections.emptyMap());
            final String fullTopicNameSoundPlayed = "de.volkerfaas.music.public.sound_played";
            final List<PartitionConfiguration> partitionsSoundPlayed = List.of(new PartitionConfiguration(fullTopicNameSoundPlayed, 0), new PartitionConfiguration(fullTopicNameSoundPlayed, 1), new PartitionConfiguration(fullTopicNameSoundPlayed, 2), new PartitionConfiguration(fullTopicNameSoundPlayed, 3), new PartitionConfiguration(fullTopicNameSoundPlayed, 4));
            final TopicConfiguration topicConfigurationSoundPlayed = new TopicConfiguration(fullTopicNameSoundPlayed, partitionsSoundPlayed, (short) 3, Collections.emptyMap());
            clusterConfiguration.getTopics().add(topicConfigurationUserUpdated);
            clusterConfiguration.getTopics().add(topicConfigurationSoundPlayed);
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();
            doNothing().when(kafkaClusterRepository).deleteTopics(anyCollection());

            final Domain domain = createTestDomain(4, Collections.emptyMap());
            final Set<String> orphanedTopics = topicService.listOrphanedTopics(List.of(domain));
            assertNotNull(orphanedTopics);
            assertEquals(1, orphanedTopics.size());
            assertThat(orphanedTopics, containsInAnyOrder(fullTopicNameSoundPlayed));
        }

        @Test
        @DisplayName("should return no orphaned topics if topology equals cluster")
        void testEmptyListOrphanedTopicsEqual() throws ExecutionException, InterruptedException {
            final String fullTopicName = "de.volkerfaas.arc.public.user_updated";
            mockTestClusterConfiguration(fullTopicName, 4, Collections.emptyMap());
            doNothing().when(kafkaClusterRepository).deleteTopics(anyCollection());

            final Domain domain = createTestDomain(4, Collections.emptyMap());
            final Set<String> orphanedTopics = topicService.listOrphanedTopics(List.of(domain));
            assertNotNull(orphanedTopics);
            assertEquals(0, orphanedTopics.size());
        }

        @Test
        @DisplayName("should return no orphaned topics if topology has more topics than cluster")
        void testEmptyListOrphanedTopicsMore() throws ExecutionException, InterruptedException {
            mockEmptyClusterConfiguration();
            doNothing().when(kafkaClusterRepository).deleteTopics(anyCollection());

            final Domain domain = createTestDomain(4, Collections.emptyMap());
            final Set<String> orphanedTopics = topicService.listOrphanedTopics(List.of(domain));
            assertNotNull(orphanedTopics);
            assertEquals(0, orphanedTopics.size());
        }

    }

    @Nested
    @DisplayName("the method listActiveConsumerGroups")
    class ListActiveConsumerGroups {

        @ParameterizedTest
        @ValueSource(strings = {
                "Unknown",
                "PreparingRebalance",
                "CompletingRebalance",
                "Stable",
                "Empty"
        })
        @DisplayName("should return active consumer groups")
        void testListActiveConsumerGroups(String state) throws ExecutionException, InterruptedException {
            final String topicName = "de.volkerfaas.test.public.user_updated";
            final String groupId = "de.volkerfaas.test.myservice";
            final List<PartitionConfiguration> partitionConfigurations = List.of(
                    new PartitionConfiguration(topicName, 0),
                    new PartitionConfiguration(topicName, 1),
                    new PartitionConfiguration(topicName, 2),
                    new PartitionConfiguration(topicName, 3)
            );
            final Map<String, String> config = new HashMap<>();
            config.put("cleanupPolicy", "compact");
            config.put("minCompactionLagMs", "100");
            final TopicConfiguration topicConfiguration = new TopicConfiguration(topicName, partitionConfigurations, (short) 3, config);
            final ConsumerGroupConfiguration consumerGroupConfiguration = new ConsumerGroupConfiguration(groupId, ConsumerGroupConfiguration.State.findByValue(state));
            final List<ConsumerConfiguration> consumerConfigurations = List.of(
                    new ConsumerConfiguration(partitionConfigurations.get(0), 9),
                    new ConsumerConfiguration(partitionConfigurations.get(1), 6),
                    new ConsumerConfiguration(partitionConfigurations.get(2), 3),
                    new ConsumerConfiguration(partitionConfigurations.get(3), 7)
            );
            consumerGroupConfiguration.getConsumers().addAll(consumerConfigurations);
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("lkc-p5zy2");
            clusterConfiguration.listTopicNames().add(topicName);
            clusterConfiguration.getTopics().add(topicConfiguration);
            clusterConfiguration.getConsumerGroups().add(consumerGroupConfiguration);
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();
            doNothing().when(kafkaClusterRepository).deleteTopics(anyCollection());

            final Collection<ConsumerGroupConfiguration> activeConsumerGroups = topicService.listActiveConsumerGroups(topicName);
            assertNotNull(activeConsumerGroups);
            assertEquals(1, activeConsumerGroups.size());
        }

        @Test
        @DisplayName("should not return dead consumer groups")
        void testNotListActiveConsumerGroups() throws ExecutionException, InterruptedException {
            final String topicName = "de.volkerfaas.test.public.user_updated";
            final String groupId = "de.volkerfaas.test.myservice";
            final List<PartitionConfiguration> partitionConfigurations = List.of(
                    new PartitionConfiguration(topicName, 0),
                    new PartitionConfiguration(topicName, 1),
                    new PartitionConfiguration(topicName, 2),
                    new PartitionConfiguration(topicName, 3)
            );
            final Map<String, String> config = new HashMap<>();
            config.put("cleanupPolicy", "compact");
            config.put("minCompactionLagMs", "100");
            final TopicConfiguration topicConfiguration = new TopicConfiguration(topicName, partitionConfigurations, (short) 3, config);
            final ConsumerGroupConfiguration consumerGroupConfiguration = new ConsumerGroupConfiguration(groupId, ConsumerGroupConfiguration.State.DEAD);
            final List<ConsumerConfiguration> consumerConfigurations = List.of(
                    new ConsumerConfiguration(partitionConfigurations.get(0), 9),
                    new ConsumerConfiguration(partitionConfigurations.get(1), 6),
                    new ConsumerConfiguration(partitionConfigurations.get(2), 3),
                    new ConsumerConfiguration(partitionConfigurations.get(3), 7)
            );
            consumerGroupConfiguration.getConsumers().addAll(consumerConfigurations);
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("lkc-p5zy2");
            clusterConfiguration.listTopicNames().add(topicName);
            clusterConfiguration.getTopics().add(topicConfiguration);
            clusterConfiguration.getConsumerGroups().add(consumerGroupConfiguration);
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();
            doNothing().when(kafkaClusterRepository).deleteTopics(anyCollection());

            final Collection<ConsumerGroupConfiguration> activeConsumerGroups = topicService.listActiveConsumerGroups(topicName);
            assertNotNull(activeConsumerGroups);
            assertEquals(0, activeConsumerGroups.size());

        }

    }

    private ClusterConfiguration mockEmptyClusterConfiguration() {
        final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("lkc-p5zy2");
        try {
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();
        } catch (InterruptedException | ExecutionException e) {
            return null;
        }

        return clusterConfiguration;
    }

    private ClusterConfiguration mockTestClusterConfiguration(String fullTopicName, int numPartitions, Map<String, String> config) {
        final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("lkc-p5zy2");
        final List<PartitionConfiguration> partitions = IntStream.range(0, numPartitions)
                .mapToObj(index -> new PartitionConfiguration(fullTopicName, index))
                .collect(Collectors.toList());
        clusterConfiguration.getTopics().add(new TopicConfiguration(fullTopicName, partitions, (short) 3, config));
        clusterConfiguration.listTopicNames().add(fullTopicName);
        try {
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();
        } catch (InterruptedException | ExecutionException e) {
           return null;
        }

        return clusterConfiguration;
    }

    private Domain createTestDomain(int numPartitions, Map<String, String> config) {
        final Domain domain = new Domain();
        domain.setName("de.volkerfaas.arc");

        final Visibility visibility = new Visibility(Visibility.Type.PUBLIC);
        visibility.setPrefix("de.volkerfaas.arc.");
        domain.getVisibilities().add(visibility);

        final Topic topic = new Topic("user_updated", numPartitions, (short) 3, config);
        topic.setPrefix("de.volkerfaas.arc.public.");
        visibility.getTopics().add(topic);

        return domain;
    }

}
