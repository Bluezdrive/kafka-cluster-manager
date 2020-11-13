package de.volkerfaas.kafka.topology.services.impl;

import de.volkerfaas.kafka.topology.ApplicationConfiguration;
import de.volkerfaas.kafka.topology.model.*;
import de.volkerfaas.kafka.topology.repositories.KafkaClusterRepository;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class TopicServiceImplTest {

    private KafkaClusterRepository kafkaClusterRepository;
    private TopicServiceImpl topicService;

    @BeforeEach
    void init() {
        this.kafkaClusterRepository = Mockito.mock(KafkaClusterRepository.class);
        this.topicService = new TopicServiceImpl(kafkaClusterRepository);
    }

    @Nested
    class createNewTopics {

        @Test
        void must_create_new_topics_from_domains() throws ExecutionException, InterruptedException {
            doAnswer(invocation -> {
                final Set<NewTopic> newTopics = invocation.getArgument(0);
                assertNotNull(newTopics);
                assertEquals(1, newTopics.size());
                final NewTopic newTopic = newTopics.stream().findFirst().orElse(null);
                assertNotNull(newTopic);
                assertEquals("de.volkerfaas.arc.public.user_updated", newTopic.name());
                return null;
            }).when(kafkaClusterRepository).createTopics(anySet());
            doReturn(new KafkaCluster("")).when(kafkaClusterRepository).dumpCluster();

            TopologyFile topology = new TopologyFile();

            Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            topology.setDomain(domain);

            Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            domain.getVisibilities().add(visibility);

            Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setNumPartitions(4);
            topic.setReplicationFactor((short) 2);
            topic.setPrefix("de.volkerfaas.arc.public.");
            visibility.getTopics().add(topic);

            topicService.createNewTopics(List.of(domain));
        }

    }

    @Nested
    class createNewPartitions {

        @Test
        void must_return_new_partitions_with_new_total_size_when_incremented() throws ExecutionException, InterruptedException {
            final KafkaCluster kafkaCluster = new KafkaCluster("");
            kafkaCluster.getTopics().add(new KafkaTopic("de.volkerfaas.arc.public.user_updated", 4, (short) 3, Collections.emptyMap()));
            doReturn(kafkaCluster).when(kafkaClusterRepository).dumpCluster();

            TopologyFile topology = new TopologyFile();

            Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            topology.setDomain(domain);

            Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            domain.getVisibilities().add(visibility);

            Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setNumPartitions(5);
            topic.setReplicationFactor((short) 2);
            topic.setPrefix("de.volkerfaas.arc.public.");
            visibility.getTopics().add(topic);

            final Map<String, NewPartitions> newPartitionsByTopic = topicService.createNewPartitions(List.of(domain));
            assertNotNull(newPartitionsByTopic);
            assertEquals(1, newPartitionsByTopic.size());
            assertTrue(newPartitionsByTopic.containsKey("de.volkerfaas.arc.public.user_updated"));

            final NewPartitions newPartitions = newPartitionsByTopic.get("de.volkerfaas.arc.public.user_updated");
            assertNotNull(newPartitions);
            assertEquals(5, newPartitions.totalCount());
        }

        @Test
        void must_return_nothing_when_not_incremented() throws ExecutionException, InterruptedException {
            final KafkaCluster kafkaCluster = new KafkaCluster("");
            kafkaCluster.getTopics().add(new KafkaTopic("de.volkerfaas.arc.public.user_updated", 4, (short) 3, Collections.emptyMap()));
            doReturn(kafkaCluster).when(kafkaClusterRepository).dumpCluster();

            TopologyFile topology = new TopologyFile();

            Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            topology.setDomain(domain);

            Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            domain.getVisibilities().add(visibility);

            Topic topic = new Topic();
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
        void must_throw_exception_when_decremented() throws ExecutionException, InterruptedException {
            final KafkaCluster kafkaCluster = new KafkaCluster("");
            kafkaCluster.getTopics().add(new KafkaTopic("de.volkerfaas.arc.public.user_updated", 4, (short) 3, Collections.emptyMap()));
            doReturn(kafkaCluster).when(kafkaClusterRepository).dumpCluster();

            TopologyFile topology = new TopologyFile();

            Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            topology.setDomain(domain);

            Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            domain.getVisibilities().add(visibility);

            Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setNumPartitions(3);
            topic.setReplicationFactor((short) 2);
            topic.setPrefix("de.volkerfaas.arc.public.");
            visibility.getTopics().add(topic);

            Exception exception = assertThrows(IllegalArgumentException.class, () -> topicService.createNewPartitions(List.of(domain)));
            assertNotNull(exception);
            assertEquals("Topic '" + topic.getFullName() + "' has smaller partitions size than in cluster.", exception.getMessage());
        }

    }

    @Nested
    class createNewTopic {

        @Test
        void must_return_a_new_topic() {
            Topic topic = new Topic();
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
    class createAlterConfigOperations {

        @Test
        void must_set_the_config_item_when_not_available_in_cluster() throws ExecutionException, InterruptedException {
            final KafkaCluster kafkaCluster = new KafkaCluster("");
            kafkaCluster.getTopics().add(new KafkaTopic("de.volkerfaas.arc.public.user_updated", 4, (short) 3, Collections.emptyMap()));
            doReturn(kafkaCluster).when(kafkaClusterRepository).dumpCluster();

            TopologyFile topology = new TopologyFile();

            Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            topology.setDomain(domain);

            Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            domain.getVisibilities().add(visibility);

            Topic topic = new Topic();
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
        void must_set_the_config_item_when_available_in_cluster_and_differs() throws ExecutionException, InterruptedException {
            final KafkaCluster kafkaCluster = new KafkaCluster("");
            Map<String, String> kafkaTopicConfig = new HashMap<>();
            kafkaTopicConfig.put("cleanupPolicy", "delete");
            kafkaCluster.getTopics().add(new KafkaTopic("de.volkerfaas.arc.public.user_updated", 4, (short) 3, kafkaTopicConfig));
            doReturn(kafkaCluster).when(kafkaClusterRepository).dumpCluster();

            TopologyFile topology = new TopologyFile();

            Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            topology.setDomain(domain);

            Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            domain.getVisibilities().add(visibility);

            Topic topic = new Topic();
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
        void must_not_set_the_config_item_when_available_in_cluster_and_equal() throws ExecutionException, InterruptedException {
            final KafkaCluster kafkaCluster = new KafkaCluster("");
            Map<String, String> kafkaTopicConfig = new HashMap<>();
            kafkaTopicConfig.put("cleanupPolicy", "compact");
            kafkaCluster.getTopics().add(new KafkaTopic("de.volkerfaas.arc.public.user_updated", 4, (short) 3, kafkaTopicConfig));
            doReturn(kafkaCluster).when(kafkaClusterRepository).dumpCluster();

            TopologyFile topology = new TopologyFile();

            Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            topology.setDomain(domain);

            Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            domain.getVisibilities().add(visibility);

            Topic topic = new Topic();
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
        void must_not_set_the_config_item_when_no_topic_available_in_cluster() throws ExecutionException, InterruptedException {
            final KafkaCluster kafkaCluster = new KafkaCluster("");
            doReturn(kafkaCluster).when(kafkaClusterRepository).dumpCluster();

            TopologyFile topology = new TopologyFile();

            Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            topology.setDomain(domain);

            Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            domain.getVisibilities().add(visibility);

            Topic topic = new Topic();
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
        void must_delete_the_config_item_in_cluster_when_not_available_local() throws ExecutionException, InterruptedException {
            final KafkaCluster kafkaCluster = new KafkaCluster("");
            Map<String, String> kafkaTopicConfig = new HashMap<>();
            kafkaTopicConfig.put("cleanupPolicy", "compact");
            kafkaCluster.getTopics().add(new KafkaTopic("de.volkerfaas.arc.public.user_updated", 4, (short) 3, kafkaTopicConfig));
            doReturn(kafkaCluster).when(kafkaClusterRepository).dumpCluster();

            TopologyFile topology = new TopologyFile();

            Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            topology.setDomain(domain);

            Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            domain.getVisibilities().add(visibility);

            Topic topic = new Topic();
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
    class isTopicAvailable {

        @Test
        void must_return_true_in_case_topic_fullname_is_in_set_of_extisting_topic_names() {
            Topic topic = new Topic();
            topic.setPrefix("de.volkerfaas.arc.public.");
            topic.setName("user_updated");

            final Set<String> existingTopicNames = Set.of("de.volkerfaas.arc.public.user_updated");
            final boolean topicAvailable = topicService.isTopicAvailable(topic, existingTopicNames);
            assertTrue(topicAvailable);
        }

        @Test
        void must_return_false_in_case_topic_fullname_is_not_in_set_of_extisting_topic_names() {
            Topic topic = new Topic();
            topic.setPrefix("de.volkerfaas.arc.public.");
            topic.setName("card_updated");

            final Set<String> existingTopicNames = Set.of("de.volkerfaas.arc.public.user_updated");
            final boolean topicAvailable = topicService.isTopicAvailable(topic, existingTopicNames);
            assertFalse(topicAvailable);
        }

    }

    @Nested
    class listTopicsInCluster {

        @Test
        void returns_kafka_topics_in_cluster() throws ExecutionException, InterruptedException {
            final KafkaCluster kafkaCluster = new KafkaCluster("");
            final KafkaTopic kafkaTopic = new KafkaTopic("de.volkerfaas.arc.public.user_updated", 4, (short) 3, Collections.emptyMap());
            kafkaCluster.getTopics().add(kafkaTopic);
            doReturn(kafkaCluster).when(kafkaClusterRepository).dumpCluster();

            final List<KafkaTopic> kafkaTopics = topicService.listTopicsInCluster();
            assertNotNull(kafkaTopics);
            assertEquals(1, kafkaTopics.size());
            assertEquals(kafkaTopic, kafkaTopics.get(0));
        }

        @Test
        void returns_empty_list_if_no_kafka_topics_in_cluster() throws ExecutionException, InterruptedException {
            final KafkaCluster kafkaCluster = new KafkaCluster("");
            doReturn(kafkaCluster).when(kafkaClusterRepository).dumpCluster();

            final List<KafkaTopic> kafkaTopics = topicService.listTopicsInCluster();
            assertNotNull(kafkaTopics);
            assertEquals(0, kafkaTopics.size());
        }

        @Test
        void returns_empty_list_if_no_kafka_cluster() throws ExecutionException, InterruptedException {
            doReturn(null).when(kafkaClusterRepository).dumpCluster();

            final List<KafkaTopic> kafkaTopics = topicService.listTopicsInCluster();
            assertNotNull(kafkaTopics);
            assertEquals(0, kafkaTopics.size());
        }

    }

}
