package de.volkerfaas.kafka.topology.utils;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class TestUtils {

    private TestUtils() {
        throw new AssertionError("No de.volkerfaas.kafka.topology.utils.TestUtils instances for you!");
    }

    public static Map<String, TopicDescription> createTopicPartitionInfos(String topicName, int numPartitions, int numReplicas) {
        final List<Node> replicas = IntStream.range(1, numReplicas + 1)
                .mapToObj(id -> new Node(id, "localhost", 9092))
                .collect(Collectors.toList());

        final HashMap<String, TopicDescription> topicPartitionInfos = new HashMap<>();
        topicPartitionInfos.put(topicName, new TopicDescription(topicName, false, IntStream.range(0, numPartitions)
                .mapToObj(partition -> new TopicPartitionInfo(partition, null, replicas, Collections.emptyList()))
                .collect(Collectors.toList())));

        return topicPartitionInfos;
    }

    public static Map<ConfigResource, Config> createConfig(String topicName, Collection<ConfigEntry> configEntries) {
        final Map<ConfigResource, Config> configs = new HashMap<>();
        final ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        final Config kafkaConfig = new Config(configEntries);
        configs.put(resource, kafkaConfig);

        return configs;
    }

    public static Map<TopicPartition, OffsetAndMetadata> createTopicPartitionOffsetAndMetadata(String topicName, int numPartitions) {
        final Random random = new Random();
        return IntStream.range(0, numPartitions)
                .mapToObj(index -> new TopicPartition(topicName, index))
                .collect(Collectors.toMap(topicPartition -> topicPartition, topicPartition -> new OffsetAndMetadata(random.nextInt(10))));
    }

    public static Set<AclBinding> createDomainAclBindings(String name, String principal) {
        final ResourcePattern resourcePatternGroup = new ResourcePattern(ResourceType.GROUP, name, PatternType.PREFIXED);
        final ResourcePattern resourcePatternTopic = new ResourcePattern(ResourceType.TOPIC, name, PatternType.PREFIXED);
        final ResourcePattern resourcePatternTransactionalId = new ResourcePattern(ResourceType.TRANSACTIONAL_ID, name, PatternType.PREFIXED);
        final ResourcePattern resourcePatternCluster = new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL);
        return new HashSet<>(Set.of(
                new AclBinding(resourcePatternGroup, new AccessControlEntry(principal, "*", AclOperation.READ, AclPermissionType.ALLOW)),
                new AclBinding(resourcePatternTopic, new AccessControlEntry(principal, "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)),
                new AclBinding(resourcePatternTopic, new AccessControlEntry(principal, "*", AclOperation.READ, AclPermissionType.ALLOW)),
                new AclBinding(resourcePatternTopic, new AccessControlEntry(principal, "*", AclOperation.WRITE, AclPermissionType.ALLOW)),
                new AclBinding(resourcePatternTransactionalId, new AccessControlEntry(principal, "*", AclOperation.WRITE, AclPermissionType.ALLOW)),
                new AclBinding(resourcePatternCluster, new AccessControlEntry(principal, "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW))
        ));
    }

    public static Set<AclBinding> createConsumerAclBindings(String name, String principal) {
        final ResourcePattern resourcePatternGroup = new ResourcePattern(ResourceType.GROUP, name, PatternType.PREFIXED);
        final ResourcePattern resourcePatternTopic = new ResourcePattern(ResourceType.TOPIC, name, PatternType.PREFIXED);
        return Set.of(
                new AclBinding(resourcePatternGroup, new AccessControlEntry(principal, "*", AclOperation.READ, AclPermissionType.ALLOW)),
                new AclBinding(resourcePatternTopic, new AccessControlEntry(principal, "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)),
                new AclBinding(resourcePatternTopic, new AccessControlEntry(principal, "*", AclOperation.READ, AclPermissionType.ALLOW))
        );
    }

}
