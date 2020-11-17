package de.volkerfaas.kafka.cluster.repositories.impl;

import de.volkerfaas.kafka.cluster.model.*;
import de.volkerfaas.kafka.cluster.repositories.KafkaClusterRepository;
import de.volkerfaas.kafka.topology.ApplicationConfiguration;
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
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Repository
public class KafkaClusterRepositoryImpl implements KafkaClusterRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClusterRepositoryImpl.class);

    private final Environment environment;
    private final AdminClient adminClient;

    @Autowired
    public KafkaClusterRepositoryImpl(Environment environment, @Lazy AdminClient adminClient) {
        this.environment = environment;
        this.adminClient = adminClient;
    }

    @Override
    public void createAccessControlLists(Collection<AclBinding> aclBindings) throws ExecutionException, InterruptedException {
        if (Objects.isNull(aclBindings) || aclBindings.isEmpty()) {
            LOGGER.info("No new ACLs to be created in cluster");
        } else if (isDryRun()) {
            LOGGER.info("New ACLs to be created in cluster: {}", aclBindings);
        } else {
            adminClient.createAcls(aclBindings).all().get();
            LOGGER.info("New ACLs created in cluster: {}", aclBindings);
        }
    }

    @Override
    public void createPartitions(Map<String, NewPartitions> newPartitions) throws ExecutionException, InterruptedException {
        if (Objects.isNull(newPartitions) || newPartitions.isEmpty()) {
            LOGGER.info("No new partitions to be created in cluster");
        } else if (isDryRun()) {
            LOGGER.info("New topics to be created in cluster: {}", newPartitions);
        } else {
            adminClient.createPartitions(newPartitions).all().get();
            LOGGER.info("New partitions created in cluster: {}", newPartitions);
        }
    }

    @Override
    public void createTopics(Collection<NewTopic> newTopics) throws ExecutionException, InterruptedException {
        if (Objects.isNull(newTopics) || newTopics.isEmpty()) {
            LOGGER.info("No new topics to be created in cluster");
        } else if (isDryRun()) {
            LOGGER.info("New topics to be created in cluster: {}", newTopics);
        } else {
            adminClient.createTopics(newTopics).all().get();
            LOGGER.info("New topics created in cluster: {}", newTopics);
        }
    }

    @Override
    public Collection<AclBinding> deleteAccessControlLists(Collection<AclBindingFilter> aclBindingFilters) throws ExecutionException, InterruptedException {
        if (Objects.isNull(aclBindingFilters) || aclBindingFilters.isEmpty()) {
            LOGGER.info("No orphaned ACLs to be removed from cluster");
        } else if (isDryRun()) {
            LOGGER.info("Orphaned ACLs to be removed from cluster: {}", aclBindingFilters);
        } else {
            final Collection<AclBinding> aclBindings = adminClient.deleteAcls(aclBindingFilters).all().get();
            LOGGER.info("Orphaned ACLs removed from cluster: {}", aclBindingFilters);

            return aclBindings;
        }

        return Collections.emptyList();
    }

    @Override
    public void deleteTopics(Collection<String> topicNames) throws ExecutionException, InterruptedException {
        if (Objects.isNull(topicNames) || topicNames.isEmpty()) {
            LOGGER.info("No orphaned topics to be removed from cluster");
        } else if (isDryRun()) {
            LOGGER.info("Orphaned topics to be removed from cluster: {}", topicNames);
        } else {
            adminClient.deleteTopics(topicNames).all().get();
            LOGGER.info("Orphaned topics removed from cluster: {}", topicNames);
        }
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
        } else if (isDryRun()) {
            LOGGER.info("Config items to be updated in cluster: {}", configs);
        } else {
            adminClient.incrementalAlterConfigs(configs).all().get();
            LOGGER.info("Config items updated in cluster: {}", configs);
        }
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

    public boolean isDryRun() {
        return environment.getRequiredProperty(ApplicationConfiguration.PROPERTY_KEY_TOPOLOGY_DRY_RUN, boolean.class);
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
                .peek(consumerGroup -> consumerGroup.getConsumers().addAll(listConsumersByConsumerGroup(consumerGroup, topics)))
                .collect(Collectors.toSet());
    }

    public Collection<ConsumerConfiguration> listConsumersByConsumerGroup(ConsumerGroupConfiguration consumerGroup, Collection<TopicConfiguration> topics) {
        try {
            final Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadata = adminClient.listConsumerGroupOffsets(consumerGroup.getGroupId())
                    .partitionsToOffsetAndMetadata()
                    .get();
            LOGGER.debug("Received consumer group offsets from Apache Kafka® cluster: {}", topicPartitionOffsetAndMetadata);

            return topicPartitionOffsetAndMetadata
                    .entrySet()
                    .stream()
                    .map(t -> getConsumer(t, topics))
                    .collect(Collectors.toList());
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
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

}
