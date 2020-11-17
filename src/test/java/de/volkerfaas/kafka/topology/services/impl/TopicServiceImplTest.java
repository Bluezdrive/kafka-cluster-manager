package de.volkerfaas.kafka.topology.services.impl;

import de.volkerfaas.kafka.cluster.model.*;
import de.volkerfaas.kafka.cluster.repositories.KafkaClusterRepository;
import de.volkerfaas.kafka.cluster.repositories.impl.KafkaClusterRepositoryImpl;
import de.volkerfaas.kafka.topology.ApplicationConfiguration;
import de.volkerfaas.kafka.topology.model.Domain;
import de.volkerfaas.kafka.topology.model.Topic;
import de.volkerfaas.kafka.topology.model.TopologyFile;
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
        this.topicService = new TopicServiceImpl(kafkaClusterRepository);
    }

    @Nested
    @DisplayName("the method createNewTopics")
    class CreateNewTopics {

        @Test
        @DisplayName("should not create a new topic if it already exists in cluster")
        void testNothing() throws ExecutionException, InterruptedException {
            final String topicName = "de.volkerfaas.arc.public.user_updated";
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("lkc-p5zy2");
            final TopicConfiguration topicConfiguration = new TopicConfiguration(topicName, Collections.emptyList(), (short) 3, Collections.emptyMap());
            clusterConfiguration.getTopics().add(topicConfiguration);
            clusterConfiguration.listTopicNames().add(topicName);
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

            final TopologyFile topology = new TopologyFile();

            final Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            topology.setDomain(domain);

            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            domain.getVisibilities().add(visibility);

            final Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setNumPartitions(4);
            topic.setReplicationFactor((short) 2);
            topic.setPrefix("de.volkerfaas.arc.public.");
            visibility.getTopics().add(topic);

            final Set<NewTopic> newTopics = topicService.createNewTopics(List.of(domain));
            assertNotNull(newTopics);
            assertEquals(0, newTopics.size());
        }

        @Test
        @DisplayName("should create a new topic if it doesn't exist in cluster")
        void testCreateNewTopic() throws ExecutionException, InterruptedException {
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("lkc-p5zy2");
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

            final TopologyFile topology = new TopologyFile();

            final Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            topology.setDomain(domain);

            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            domain.getVisibilities().add(visibility);

            final Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setNumPartitions(4);
            topic.setReplicationFactor((short) 2);
            topic.setPrefix("de.volkerfaas.arc.public.");
            visibility.getTopics().add(topic);

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
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("lkc-p5zy2");
            final String fullTopicName = "de.volkerfaas.arc.public.user_updated";
            final List<PartitionConfiguration> partitions = List.of(new PartitionConfiguration(fullTopicName, 0), new PartitionConfiguration(fullTopicName, 1), new PartitionConfiguration(fullTopicName, 2), new PartitionConfiguration(fullTopicName, 3));
            clusterConfiguration.getTopics().add(new TopicConfiguration(fullTopicName, partitions, (short) 3, Collections.emptyMap()));
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

            final TopologyFile topology = new TopologyFile();

            final Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            topology.setDomain(domain);

            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            domain.getVisibilities().add(visibility);

            final Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setNumPartitions(5);
            topic.setReplicationFactor((short) 2);
            topic.setPrefix("de.volkerfaas.arc.public.");
            visibility.getTopics().add(topic);

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
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("lkc-p5zy2");
            final String fullTopicName = "de.volkerfaas.arc.public.user_updated";
            final List<PartitionConfiguration> partitions = List.of(new PartitionConfiguration(fullTopicName, 0), new PartitionConfiguration(fullTopicName, 1), new PartitionConfiguration(fullTopicName, 2), new PartitionConfiguration(fullTopicName, 3));
            clusterConfiguration.getTopics().add(new TopicConfiguration(fullTopicName, partitions, (short) 3, Collections.emptyMap()));
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

            final TopologyFile topology = new TopologyFile();

            final Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            topology.setDomain(domain);

            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            domain.getVisibilities().add(visibility);

            final Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setNumPartitions(4);
            topic.setReplicationFactor((short) 2);
            topic.setPrefix("de.volkerfaas.arc.public.");
            visibility.getTopics().add(topic);

            final Map<String, NewPartitions> newPartitionsByTopic = topicService.createNewPartitions(List.of(domain));
            assertNotNull(newPartitionsByTopic);
            assertEquals(0, newPartitionsByTopic.size());
            assertFalse(newPartitionsByTopic.containsKey("de.volkerfaas.arc.public.user_updated"));
        }

        @Test
        @DisplayName("should not create a new partition if number of partitions in topology is less than cluster")
        void testNotCreateNewPartitionWhenLess() throws ExecutionException, InterruptedException {
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("lkc-p5zy2");
            final String fullTopicName = "de.volkerfaas.arc.public.user_updated";
            final List<PartitionConfiguration> partitions = List.of(new PartitionConfiguration(fullTopicName, 0), new PartitionConfiguration(fullTopicName, 1), new PartitionConfiguration(fullTopicName, 2), new PartitionConfiguration(fullTopicName, 3));
            clusterConfiguration.getTopics().add(new TopicConfiguration(fullTopicName, partitions, (short) 3, Collections.emptyMap()));
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

            final TopologyFile topology = new TopologyFile();

            final Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            topology.setDomain(domain);

            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            domain.getVisibilities().add(visibility);

            final Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setNumPartitions(3);
            topic.setReplicationFactor((short) 2);
            topic.setPrefix("de.volkerfaas.arc.public.");
            visibility.getTopics().add(topic);

            final Exception exception = assertThrows(IllegalArgumentException.class, () -> topicService.createNewPartitions(List.of(domain)));
            assertNotNull(exception);
            assertEquals("Topic '" + topic.getFullName() + "' has smaller partitions size than in cluster.", exception.getMessage());
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
            topic.setValueSchemaFile("events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.avsc");
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
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("lkc-p5zy2");
            final String fullTopicName = "de.volkerfaas.arc.public.user_updated";
            final List<PartitionConfiguration> partitions = List.of(new PartitionConfiguration(fullTopicName, 0), new PartitionConfiguration(fullTopicName, 1), new PartitionConfiguration(fullTopicName, 2), new PartitionConfiguration(fullTopicName, 3));
            clusterConfiguration.getTopics().add(new TopicConfiguration(fullTopicName, partitions, (short) 3, Collections.emptyMap()));
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

            final TopologyFile topology = new TopologyFile();

            final Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            topology.setDomain(domain);

            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            domain.getVisibilities().add(visibility);

            final Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setNumPartitions(4);
            topic.setReplicationFactor((short) 2);
            topic.setPrefix("de.volkerfaas.arc.public.");
            topic.getConfig().put("cleanupPolicy", "compact");
            visibility.getTopics().add(topic);

            final ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic.getFullName());
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
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("lkc-p5zy2");
            final String fullTopicName = "de.volkerfaas.arc.public.user_updated";
            final List<PartitionConfiguration> partitions = List.of(new PartitionConfiguration(fullTopicName, 0), new PartitionConfiguration(fullTopicName, 1), new PartitionConfiguration(fullTopicName, 2), new PartitionConfiguration(fullTopicName, 3));
            final Map<String, String> config = new HashMap<>();
            config.put("cleanupPolicy", "delete");
            clusterConfiguration.getTopics().add(new TopicConfiguration(fullTopicName, partitions, (short) 3, config));
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

            final TopologyFile topology = new TopologyFile();

            final Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            topology.setDomain(domain);

            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            domain.getVisibilities().add(visibility);

            final Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setNumPartitions(4);
            topic.setReplicationFactor((short) 2);
            topic.setPrefix("de.volkerfaas.arc.public.");
            topic.getConfig().put("cleanupPolicy", "compact");
            visibility.getTopics().add(topic);

            final ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic.getFullName());
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
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("lkc-p5zy2");
            final String fullTopicName = "de.volkerfaas.arc.public.user_updated";
            final List<PartitionConfiguration> partitions = List.of(new PartitionConfiguration(fullTopicName, 0), new PartitionConfiguration(fullTopicName, 1), new PartitionConfiguration(fullTopicName, 2), new PartitionConfiguration(fullTopicName, 3));
            final Map<String, String> config = new HashMap<>();
            config.put("cleanupPolicy", "compact");
            clusterConfiguration.getTopics().add(new TopicConfiguration(fullTopicName, partitions, (short) 3, config));
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

            final TopologyFile topology = new TopologyFile();

            final Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            topology.setDomain(domain);

            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            domain.getVisibilities().add(visibility);

            final Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setNumPartitions(4);
            topic.setReplicationFactor((short) 2);
            topic.setPrefix("de.volkerfaas.arc.public.");
            topic.getConfig().put("cleanupPolicy", "compact");
            visibility.getTopics().add(topic);

            final ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic.getFullName());
            final Map<ConfigResource, Collection<AlterConfigOp>> configs = topicService.createAlterConfigOperations(List.of(domain));
            assertNotNull(configs);
            assertFalse(configs.containsKey(configResource));
        }

        @Test
        @DisplayName("should not update config entry if topic not exists")
        void testNotAlterConfigOperationsTopicMissing() throws ExecutionException, InterruptedException {
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("lkc-p5zy2");
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

            final TopologyFile topology = new TopologyFile();

            final Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            topology.setDomain(domain);

            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            domain.getVisibilities().add(visibility);

            final Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setNumPartitions(4);
            topic.setReplicationFactor((short) 2);
            topic.setPrefix("de.volkerfaas.arc.public.");
            topic.getConfig().put("cleanupPolicy", "compact");
            visibility.getTopics().add(topic);

            final ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic.getFullName());
            final Map<ConfigResource, Collection<AlterConfigOp>> configs = topicService.createAlterConfigOperations(List.of(domain));
            assertNotNull(configs);
            assertFalse(configs.containsKey(configResource));
        }

        @Test
        @DisplayName("should delete config entry if not exists in topology")
        void testDeleteAlterConfigOperations() throws ExecutionException, InterruptedException {
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("lkc-p5zy2");
            final String fullTopicName = "de.volkerfaas.arc.public.user_updated";
            final List<PartitionConfiguration> partitions = List.of(new PartitionConfiguration(fullTopicName, 0), new PartitionConfiguration(fullTopicName, 1), new PartitionConfiguration(fullTopicName, 2), new PartitionConfiguration(fullTopicName, 3));
            final Map<String, String> config = new HashMap<>();
            config.put("cleanupPolicy", "compact");
            clusterConfiguration.getTopics().add(new TopicConfiguration(fullTopicName, partitions, (short) 3, config));
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

            final TopologyFile topology = new TopologyFile();

            final Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            topology.setDomain(domain);

            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            domain.getVisibilities().add(visibility);

            final Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setNumPartitions(4);
            topic.setReplicationFactor((short) 2);
            topic.setPrefix("de.volkerfaas.arc.public.");
            visibility.getTopics().add(topic);

            final ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic.getFullName());
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
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("lkc-p5zy2");
            final String fullTopicName = "de.volkerfaas.arc.public.user_updated";
            final List<PartitionConfiguration> partitions = List.of(new PartitionConfiguration(fullTopicName, 0), new PartitionConfiguration(fullTopicName, 1), new PartitionConfiguration(fullTopicName, 2), new PartitionConfiguration(fullTopicName, 3));
            final TopicConfiguration topicConfiguration = new TopicConfiguration(fullTopicName, partitions, (short) 3, Collections.emptyMap());
            clusterConfiguration.getTopics().add(topicConfiguration);
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

            final Collection<TopicConfiguration> topicConfigurations = topicService.listTopicsInCluster();
            assertNotNull(topicConfigurations);
            assertEquals(1, topicConfigurations.size());
            assertTrue(topicConfigurations.contains(topicConfiguration));
        }

        @Test
        @DisplayName("should return an empty list if no topics available in cluster")
        void testEmptyListTopicsInClusterNoTopics() throws ExecutionException, InterruptedException {
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("lkc-p5zy2");
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

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

            final Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");

            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            domain.getVisibilities().add(visibility);

            final Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setNumPartitions(4);
            topic.setReplicationFactor((short) 3);
            topic.setPrefix("de.volkerfaas.arc.public.");
            visibility.getTopics().add(topic);

            final Set<String> orphanedTopics = topicService.listOrphanedTopics(List.of(domain));
            assertNotNull(orphanedTopics);
            assertEquals(1, orphanedTopics.size());
            assertThat(orphanedTopics, containsInAnyOrder(fullTopicNameSoundPlayed));
        }

        @Test
        @DisplayName("should return no orphaned topics if topology equals cluster")
        void testEmptyListOrphanedTopicsEqual() throws ExecutionException, InterruptedException {
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("lkc-p5zy2");
            final String fullTopicName = "de.volkerfaas.arc.public.user_updated";
            final List<PartitionConfiguration> partitions = List.of(new PartitionConfiguration(fullTopicName, 0), new PartitionConfiguration(fullTopicName, 1), new PartitionConfiguration(fullTopicName, 2), new PartitionConfiguration(fullTopicName, 3));
            final TopicConfiguration topicConfigurationUserUpdated = new TopicConfiguration(fullTopicName, partitions, (short) 3, Collections.emptyMap());
            clusterConfiguration.getTopics().add(topicConfigurationUserUpdated);
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();
            doNothing().when(kafkaClusterRepository).deleteTopics(anyCollection());

            final Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");

            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            domain.getVisibilities().add(visibility);

            final Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setNumPartitions(4);
            topic.setReplicationFactor((short) 3);
            topic.setPrefix("de.volkerfaas.arc.public.");
            visibility.getTopics().add(topic);

            final Set<String> orphanedTopics = topicService.listOrphanedTopics(List.of(domain));
            assertNotNull(orphanedTopics);
            assertEquals(0, orphanedTopics.size());
        }

        @Test
        @DisplayName("should return no orphaned topics if topology has more topics than cluster")
        void testEmptyListOrphanedTopicsMore() throws ExecutionException, InterruptedException {
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("lkc-p5zy2");
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();
            doNothing().when(kafkaClusterRepository).deleteTopics(anyCollection());

            final Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");

            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            domain.getVisibilities().add(visibility);

            final Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setNumPartitions(4);
            topic.setReplicationFactor((short) 3);
            topic.setPrefix("de.volkerfaas.arc.public.");
            visibility.getTopics().add(topic);

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

}
