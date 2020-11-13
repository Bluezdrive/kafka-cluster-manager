package de.volkerfaas.kafka.topology.repositories.impl;

import de.volkerfaas.kafka.topology.model.KafkaCluster;
import de.volkerfaas.kafka.topology.model.KafkaTopic;
import de.volkerfaas.kafka.topology.repositories.KafkaClusterRepository;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.*;
import org.springframework.core.env.Environment;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class KafkaClusterRepositoryImplTest {

    private AdminClient adminClient;
    private KafkaClusterRepository kafkaClusterRepository;

    @BeforeEach
    void init() {
        this.adminClient = mock(AdminClient.class);
        Environment environment = mock(Environment.class);
        this.kafkaClusterRepository = new KafkaClusterRepositoryImpl(environment, adminClient);
    }

    @Nested
    class dumpCluster {

        @Test
        void test() throws ExecutionException, InterruptedException {
            final String topicName = "de.volkerfaas.test.public.user_updated";
            Set<String> topicNames = Set.of(topicName);

            final KafkaFuture<Set<AclBinding>> kafkaFutureAclBindings = mock(KafkaFuture.class);
            final DescribeAclsResult describeAclsResult = mock(DescribeAclsResult.class);
            doReturn(Collections.emptyList()).when(kafkaFutureAclBindings).get();
            doReturn(kafkaFutureAclBindings).when(describeAclsResult).values();
            doReturn(describeAclsResult).when(adminClient).describeAcls(any());

            final KafkaFuture<Set<String>> kafkaFutureNames = mock(KafkaFuture.class);
            final ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
            doReturn(topicNames).when(kafkaFutureNames).get();
            doReturn(kafkaFutureNames).when(listTopicsResult).names();
            doReturn(listTopicsResult).when(adminClient).listTopics();

            final KafkaFuture<String> kafkaFutureClusterId = mock(KafkaFuture.class);
            final DescribeClusterResult describeClusterResult = mock(DescribeClusterResult.class);
            doReturn("lkc-p5zy2").when(kafkaFutureClusterId).get();
            doReturn(kafkaFutureClusterId).when(describeClusterResult).clusterId();
            doReturn(describeClusterResult).when(adminClient).describeCluster();

            final List<Node> replicas = List.of(new Node(1, "localhost", 9092), new Node(2, "localhost", 9092), new Node(3, "localhost", 9092), new Node(4, "localhost", 9092), new Node(5, "localhost", 9092));
            final List<TopicPartitionInfo> topicPartitionInfos = List.of(
                    new TopicPartitionInfo(0, null, replicas, Collections.emptyList()),
                    new TopicPartitionInfo(1, null, replicas, Collections.emptyList()),
                    new TopicPartitionInfo(2, null, replicas, Collections.emptyList()),
                    new TopicPartitionInfo(3, null, replicas, Collections.emptyList())
            );
            final TopicDescription topicDescription = new TopicDescription(topicName, false, topicPartitionInfos);
            final Map<String, TopicDescription> topicDescriptions = new HashMap<>();
            topicDescriptions.put(topicName, topicDescription);

            final KafkaFuture<Map<String, TopicDescription>> kafkaFutureTopicDescription = mock(KafkaFuture.class);
            final DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
            doReturn(topicDescriptions).when(kafkaFutureTopicDescription).get();
            doReturn(kafkaFutureTopicDescription).when(describeTopicsResult).all();
            doReturn(describeTopicsResult).when(adminClient).describeTopics(eq(topicNames));

            Map<ConfigResource, Config> configMap = new HashMap<>();
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            Config kafkaConfig = new Config(Set.of(
                    ConfigEntries.createDynamicTopicConfigEntry("cleanup.policy", "compact"),
                    ConfigEntries.createDynamicTopicConfigEntry("min.compaction.lag.ms", "100")
            ));
            configMap.put(resource, kafkaConfig);

            final KafkaFuture<Map<ConfigResource, Config>> kafkaFutureConfig = mock(KafkaFuture.class);
            final DescribeConfigsResult describeConfigsResult = mock(DescribeConfigsResult.class);
            doReturn(configMap).when(kafkaFutureConfig).get();
            doReturn(kafkaFutureConfig).when(describeConfigsResult).all();
            doReturn(describeConfigsResult).when(adminClient).describeConfigs(anySet());

            final KafkaCluster kafkaCluster = kafkaClusterRepository.dumpCluster();
            assertNotNull(kafkaCluster);
            assertEquals("lkc-p5zy2", kafkaCluster.getClusterId());

            final KafkaTopic kafkaTopic = kafkaCluster.getTopics().get(0);
            assertNotNull(kafkaTopic);
            assertEquals(topicName, kafkaTopic.getName());
            assertEquals(4, kafkaTopic.getNumPartitions());
            assertEquals(5, kafkaTopic.getReplicationFactor());

            final Map<String, String> config = kafkaTopic.getConfig();
            assertNotNull(config);
            assertEquals(2, config.size());

            final String cleanupPolicy = config.get("cleanupPolicy");
            assertNotNull(cleanupPolicy);
            assertEquals("compact", cleanupPolicy);

            final String minCompactionLagMs = config.get("minCompactionLagMs");
            assertNotNull(minCompactionLagMs);
            assertEquals("100", minCompactionLagMs);
        }

    }

}
