package de.volkerfaas.kafka.cluster.repositories.impl;

import com.github.freva.asciitable.Column;
import de.volkerfaas.kafka.cluster.model.*;
import de.volkerfaas.kafka.cluster.repositories.KafkaClusterRepository;
import de.volkerfaas.kafka.topology.utils.ConfigEntryUtils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.github.freva.asciitable.AsciiTable.getTable;
import static com.github.freva.asciitable.HorizontalAlign.LEFT;
import static de.volkerfaas.utils.ExceptionUtils.handleException;

@Repository
public class KafkaClusterRepositoryImpl implements KafkaClusterRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClusterRepositoryImpl.class);

    private final AdminClient adminClient;
    private final boolean dryRun;

    @Autowired
    public KafkaClusterRepositoryImpl(@Lazy AdminClient adminClient, @Value("${dry-run:@null}") String dryRun) {
        this.adminClient = adminClient;
        this.dryRun = Objects.nonNull(dryRun);
    }

    @Override
    public void createAccessControlLists(Collection<AclBinding> aclBindings) throws ExecutionException, InterruptedException {
        if (Objects.isNull(aclBindings) || aclBindings.isEmpty()) {
            LOGGER.info("No new ACLs to be created in cluster");
        } else if (dryRun) {
            LOGGER.info("New ACLs to be created in cluster");
        } else {
            adminClient.createAcls(aclBindings).all().get();
            LOGGER.info("New ACLs created in cluster");
        }
        printAclBindings(aclBindings);
    }

    @Override
    public void createPartitions(Map<String, NewPartitions> newPartitions) throws ExecutionException, InterruptedException {
        if (Objects.isNull(newPartitions) || newPartitions.isEmpty()) {
            LOGGER.info("No new partitions to be created in cluster");
        } else if (dryRun) {
            LOGGER.info("New partitions to be created in cluster");
        } else {
            adminClient.createPartitions(newPartitions).all().get();
            LOGGER.info("New partitions created in cluster");
        }
        printNewPartitions(newPartitions);
    }

    @Override
    public void createTopics(Collection<NewTopic> newTopics) throws ExecutionException, InterruptedException {
        if (Objects.isNull(newTopics) || newTopics.isEmpty()) {
            LOGGER.info("No new topics to be created in cluster");
        } else if (dryRun) {
            LOGGER.info("New topics to be created in cluster");
        } else {
            adminClient.createTopics(newTopics).all().get();
            LOGGER.info("New topics created in cluster");
        }
        printNewTopics(newTopics);
    }

    @Override
    public Collection<AclBinding> deleteAccessControlLists(Collection<AclBindingFilter> aclBindingFilters) throws ExecutionException, InterruptedException {
        Collection<AclBinding> aclBindings = Collections.emptyList();
        if (Objects.isNull(aclBindingFilters) || aclBindingFilters.isEmpty()) {
            LOGGER.info("No orphaned ACLs to be removed from cluster");
        } else if (dryRun) {
            LOGGER.info("Orphaned ACLs to be removed from cluster");
            printAclBindingFilters(aclBindingFilters);
        } else {
            aclBindings = adminClient.deleteAcls(aclBindingFilters).all().get();
            LOGGER.info("Orphaned ACLs removed from cluster");
            printAclBindings(aclBindings);
        }

        return Collections.unmodifiableCollection(aclBindings);
    }

    @Override
    public void deleteTopics(Collection<String> topicNames) throws ExecutionException, InterruptedException {
        if (Objects.isNull(topicNames) || topicNames.isEmpty()) {
            LOGGER.info("No orphaned topics to be removed from cluster");
        } else if (dryRun) {
            LOGGER.info("Orphaned topics to be removed from cluster");
        } else {
            adminClient.deleteTopics(topicNames).all().get();
            LOGGER.info("Orphaned topics removed from cluster");
        }
        printTopicsNames(topicNames);
    }

    @Override
    @Cacheable("cluster")
    public ClusterConfiguration getClusterConfiguration() throws ExecutionException, InterruptedException {
        final String clusterId = adminClient.describeCluster().clusterId().get();
        LOGGER.info("Connected to Apache Kafka® cluster '{}'", clusterId);

        final Set<String> topicNames = listTopicNames();
        final List<TopicConfiguration> topics = listTopicsByNames(topicNames);
        final List<AclBinding> aclBindings = listAccessControlLists(ResourceType.ANY, null);
        final Collection<ConsumerGroupConfiguration> consumerGroups = listConsumerGroups(topics);

        return new ClusterConfiguration(clusterId, topics, aclBindings, consumerGroups);
    }

    @Override
    public List<AclBinding> listAccessControlLists(ResourceType resourceType, String name) throws ExecutionException, InterruptedException {
        final ResourcePatternFilter resourcePatternFilter = new ResourcePatternFilter(resourceType, name, PatternType.ANY);
        final AclBindingFilter aclBindingFilter = new AclBindingFilter(resourcePatternFilter, AccessControlEntryFilter.ANY);
        final Collection<AclBinding> aclBindings = adminClient
                .describeAcls(aclBindingFilter)
                .values()
                .get();

        LOGGER.debug("Received access control lists from Apache Kafka® cluster for resource '{}': {}", name != null ? name : "any", aclBindings);

        return aclBindings.stream()
                .filter(aclBinding -> name == null || Objects.equals(name, aclBinding.pattern().name()))
                .collect(Collectors.toList());
    }

    @Override
    public void updateConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs) throws ExecutionException, InterruptedException {
        if (Objects.isNull(configs) || configs.isEmpty()) {
            LOGGER.info("No config items to be updated in cluster");
        } else if (dryRun) {
            LOGGER.info("Config items to be updated in cluster");
        } else {
            adminClient.incrementalAlterConfigs(configs).all().get();
            LOGGER.info("Config items updated in cluster");
        }
        printConfigs(configs);
    }

    public void addOffsetOfPartitionsToTopics(List<TopicConfiguration> topics, Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> topicPartitionListOffsetsResultInfos) {
        topics.stream()
                .map(TopicConfiguration::getPartitions)
                .flatMap(List::stream)
                .forEach(partition -> {
                    final TopicPartition topicPartition = new TopicPartition(partition.getTopicName(), partition.getIndex());
                    final ListOffsetsResult.ListOffsetsResultInfo listOffsetsResultInfo = topicPartitionListOffsetsResultInfos.get(topicPartition);
                    if (Objects.nonNull(listOffsetsResultInfo)) {
                        partition.setOffset(listOffsetsResultInfo.offset());
                        partition.setTimestamp(listOffsetsResultInfo.timestamp());
                    }
                });
    }

    public Map<String, String> getConfig(Config config) {
        return config.entries().stream()
                .filter(this::isDynamicTopicConfig)
                .collect(Collectors.toMap(ConfigEntryUtils::getCamelCase, ConfigEntry::value));
    }

    public ConsumerConfiguration getConsumer(Map.Entry<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadata, Collection<TopicConfiguration> topics) {
        final String topicName = topicPartitionOffsetAndMetadata.getKey().topic();
        final int index = topicPartitionOffsetAndMetadata.getKey().partition();
        final long offset = topicPartitionOffsetAndMetadata.getValue().offset();
        final PartitionConfiguration partition = topics.stream()
                .map(TopicConfiguration::getPartitions)
                .flatMap(List::stream)
                .filter(p -> Objects.equals(p.getTopicName(), topicName) && Objects.equals(p.getIndex(), index))
                .findFirst()
                .orElse(null);

        return new ConsumerConfiguration(partition, offset);
    }

    public List<PartitionConfiguration> getPartitions(TopicDescription description) {
        return description.partitions().stream()
                .map(partition -> new PartitionConfiguration(description.name(), partition.partition()))
                .collect(Collectors.toList());
    }

    public short getReplicationFactor(TopicDescription topicDescription) {
        return (short) topicDescription
                .partitions()
                .get(0)
                .replicas()
                .size();
    }

    public TopicConfiguration getTopicByDescriptionAndConfig(TopicDescription description, Config config) {
        return new TopicConfiguration(description.name(), getPartitions(description), getReplicationFactor(description), getConfig(config));
    }

    public Map<TopicPartition, OffsetSpec> getTopicPartitionOffsetSpecs(List<TopicConfiguration> topics) {
        return topics.stream()
                .map(TopicConfiguration::getPartitions)
                .flatMap(List::stream)
                .collect(Collectors.toMap(p -> new TopicPartition(p.getTopicName(), p.getIndex()), p -> OffsetSpec.latest()));
    }

    public boolean isDynamicTopicConfig(ConfigEntry configEntry) {
        return configEntry.source().equals(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG);
    }

    public Map<String, Config> listConfigsByNames(Set<String> names) throws ExecutionException, InterruptedException {
        final Set<ConfigResource> resources = names.stream()
                .map(name -> new ConfigResource(ConfigResource.Type.TOPIC, name))
                .collect(Collectors.toSet());
        final Map<ConfigResource, Config> configs = adminClient
                .describeConfigs(resources)
                .all()
                .get();
        LOGGER.debug("Received topic configs from Apache Kafka® cluster: {}", configs);

        return configs.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> e.getKey().name(),
                        Map.Entry::getValue
                ));
    }

    public Collection<String> listConsumerGroupIds() throws InterruptedException, ExecutionException {
        final Collection<ConsumerGroupListing> consumerGroupListings = adminClient.listConsumerGroups()
                .all()
                .get();
        LOGGER.debug("Received consumer group ids from Apache Kafka® cluster: {}", consumerGroupListings);

        return consumerGroupListings
                .stream()
                .map(ConsumerGroupListing::groupId)
                .collect(Collectors.toSet());
    }

    public Collection<ConsumerGroupConfiguration> listConsumerGroups(Collection<TopicConfiguration> topics) throws ExecutionException, InterruptedException {
        final Collection<String> groupIds = listConsumerGroupIds();
        if (Objects.isNull(groupIds) || groupIds.isEmpty()) {
            return Collections.emptySet();
        }

        final Map<String, ConsumerGroupDescription> consumerGroupDescriptions = adminClient.describeConsumerGroups(groupIds)
                .all()
                .get();
        LOGGER.debug("Received consumer group descriptions from Apache Kafka® cluster: {}", consumerGroupDescriptions);

        return consumerGroupDescriptions
                .values()
                .stream()
                .map(description -> new ConsumerGroupConfiguration(description.groupId(), ConsumerGroupConfiguration.State.findByValue(description.state().toString())))
                .peek(consumerGroup -> consumerGroup.getConsumers().addAll(handleException(() -> listConsumersByConsumerGroup(consumerGroup, topics))))
                .collect(Collectors.toSet());
    }

    public Collection<ConsumerConfiguration> listConsumersByConsumerGroup(ConsumerGroupConfiguration consumerGroup, Collection<TopicConfiguration> topics) throws ExecutionException, InterruptedException {
        final Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadata = adminClient.listConsumerGroupOffsets(consumerGroup.getGroupId())
                .partitionsToOffsetAndMetadata()
                .get();
        LOGGER.debug("Received consumer group offsets from Apache Kafka® cluster: {}", topicPartitionOffsetAndMetadata);

        return topicPartitionOffsetAndMetadata
                .entrySet()
                .stream()
                .map(t -> getConsumer(t, topics))
                .collect(Collectors.toList());
    }

    public Set<String> listTopicNames() throws ExecutionException, InterruptedException {
        final Set<String> topicNames = adminClient
                .listTopics()
                .names()
                .get();
        LOGGER.debug("Received topic names from Apache Kafka® cluster: {}", topicNames);

        return topicNames;
    }

    public List<TopicConfiguration> listTopicsByDescriptionsAndConfigs(Collection<TopicDescription> descriptions, Map<String, Config> configs) {
        return descriptions.stream()
                .map(topicDescription -> getTopicByDescriptionAndConfig(topicDescription, configs.get(topicDescription.name())))
                .collect(Collectors.toList());
    }

    public List<TopicConfiguration> listTopicsByNames(Set<String> names) throws ExecutionException, InterruptedException {
        final Map<String, TopicDescription> topicDescriptions = adminClient.describeTopics(names).all().get();
        LOGGER.debug("Received topic descriptions from Apache Kafka® cluster: {}", topicDescriptions);
        final Map<String, Config> configs = listConfigsByNames(names);
        LOGGER.debug("Received topic configurations from Apache Kafka® cluster: {}", configs);

        final List<TopicConfiguration> topics = listTopicsByDescriptionsAndConfigs(topicDescriptions.values(), configs);
        final Map<TopicPartition, OffsetSpec> topicPartitionOffsetSpecs = getTopicPartitionOffsetSpecs(topics);
        final Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> topicPartitionListOffsetsResultInfos = adminClient.listOffsets(topicPartitionOffsetSpecs).all().get();
        LOGGER.debug("Received topic partition offsets from Apache Kafka® cluster: {}", configs);
        addOffsetOfPartitionsToTopics(topics, topicPartitionListOffsetsResultInfos);

        return topics;
    }

    private void printAclBindings(Collection<AclBinding> aclBindings) {
        if (Objects.isNull(aclBindings) || aclBindings.isEmpty()) {
            return;
        }
        System.out.println(getTable(aclBindings, Arrays.asList(
                new Column().header("Principal").dataAlign(LEFT).with(aclBinding -> aclBinding.entry().principal()),
                new Column().header("Permission").dataAlign(LEFT).with(aclBinding -> aclBinding.entry().permissionType().toString()),
                new Column().header("Operation").dataAlign(LEFT).with(aclBinding -> aclBinding.entry().operation().toString()),
                new Column().header("Resource").dataAlign(LEFT).with(aclBinding -> aclBinding.pattern().resourceType().toString()),
                new Column().header("Name").dataAlign(LEFT).with(aclBinding -> aclBinding.pattern().name()),
                new Column().header("Type").dataAlign(LEFT).with(aclBinding -> aclBinding.pattern().patternType().toString())
        )));
    }

    private void printAclBindingFilters(Collection<AclBindingFilter> aclBindingFilters) {
        if (Objects.isNull(aclBindingFilters) || aclBindingFilters.isEmpty()) {
            return;
        }
        System.out.println(getTable(aclBindingFilters, Arrays.asList(
                new Column().header("Principal").dataAlign(LEFT).with(aclBinding -> aclBinding.entryFilter().principal()),
                new Column().header("Permission").dataAlign(LEFT).with(aclBinding -> aclBinding.entryFilter().permissionType().toString()),
                new Column().header("Operation").dataAlign(LEFT).with(aclBinding -> aclBinding.entryFilter().operation().toString()),
                new Column().header("Resource").dataAlign(LEFT).with(aclBinding -> aclBinding.patternFilter().resourceType().toString()),
                new Column().header("Name").dataAlign(LEFT).with(aclBinding -> aclBinding.patternFilter().name()),
                new Column().header("Type").dataAlign(LEFT).with(aclBinding -> aclBinding.patternFilter().patternType().toString())
        )));
    }

    private void printConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs) {
        if (Objects.isNull(configs) || configs.isEmpty()) {
            return;
        }
        System.out.println(getTable(configs.entrySet(), Arrays.asList(
                new Column().header("Topic").dataAlign(LEFT).with(entry -> entry.getKey().name()),
                new Column().header("Config").dataAlign(LEFT).with(entry -> entry.getValue()
                        .stream()
                        .map(alterConfigOp -> alterConfigOp.opType() + " " + alterConfigOp.configEntry().name() + "=" + alterConfigOp.configEntry().value())
                        .collect(Collectors.joining(System.lineSeparator())
                        ))

        )));
    }

    private void printNewPartitions(Map<String, NewPartitions> newPartitions) {
        if (Objects.isNull(newPartitions) || newPartitions.isEmpty()) {
            return;
        }
        System.out.println(getTable(newPartitions.entrySet(), Arrays.asList(
                new Column().header("Topic").dataAlign(LEFT).with(Map.Entry::getKey),
                new Column().header("Partitions").dataAlign(LEFT).with(entry -> String.valueOf(entry.getValue().totalCount()))

        )));
    }

    private void printNewTopics(Collection<NewTopic> newTopics) {
        if (Objects.isNull(newTopics) || newTopics.isEmpty()) {
            return;
        }
        System.out.println(getTable(newTopics, Arrays.asList(
                new Column().header("Name").dataAlign(LEFT).with(NewTopic::name),
                new Column().header("Partitions").dataAlign(LEFT).with(newTopic -> String.valueOf(newTopic.numPartitions())),
                new Column().header("Replication Factor").dataAlign(LEFT).with(newTopic -> String.valueOf(newTopic.replicationFactor())),
                new Column().header("Config").dataAlign(LEFT).with(newTopic -> (Objects.nonNull(newTopic.configs()) ? newTopic.configs() : Collections.emptyMap())
                        .entrySet()
                        .stream()
                        .map(entry -> entry.getKey() + "=" + entry.getValue())
                        .collect(Collectors.joining(System.lineSeparator())
                        ))
        )));
    }

    private void printTopicsNames(Collection<String> topicNames) {
        if (Objects.isNull(topicNames) || topicNames.isEmpty()) {
            return;
        }
        System.out.println(getTable(topicNames, Collections.singletonList(
                new Column().header("Name").dataAlign(LEFT).with(String::toString)
        )));
    }

}
