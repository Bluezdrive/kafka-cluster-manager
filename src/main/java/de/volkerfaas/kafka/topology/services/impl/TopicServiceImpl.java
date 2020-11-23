package de.volkerfaas.kafka.topology.services.impl;

import de.volkerfaas.kafka.cluster.model.ClusterConfiguration;
import de.volkerfaas.kafka.cluster.model.ConsumerGroupConfiguration;
import de.volkerfaas.kafka.cluster.model.TopicConfiguration;
import de.volkerfaas.kafka.cluster.repositories.KafkaClusterRepository;
import de.volkerfaas.kafka.topology.model.AccessControl;
import de.volkerfaas.kafka.topology.model.Domain;
import de.volkerfaas.kafka.topology.model.Topic;
import de.volkerfaas.kafka.topology.model.Visibility;
import de.volkerfaas.kafka.topology.services.TopicService;
import de.volkerfaas.kafka.topology.utils.ConfigEntryUtils;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static de.volkerfaas.kafka.topology.ApplicationConfiguration.REGEX_FULL_TOPIC_NAME;

@Service
public class TopicServiceImpl implements TopicService {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicServiceImpl.class);
    private static final Pattern PATTERN_FULL_TOPIC_NAME = Pattern.compile(REGEX_FULL_TOPIC_NAME);

    private final KafkaClusterRepository kafkaClusterRepository;

    @Autowired
    public TopicServiceImpl(final KafkaClusterRepository kafkaClusterRepository) {
        this.kafkaClusterRepository = kafkaClusterRepository;
    }

    @Override
    public Map<ConfigResource, Collection<AlterConfigOp>> createAlterConfigOperations(final Collection<Domain> domains) throws ExecutionException, InterruptedException {
        final ClusterConfiguration clusterConfiguration = kafkaClusterRepository.getClusterConfiguration();
        return domains.stream()
                .map(Domain::getVisibilities)
                .flatMap(List::stream)
                .map(Visibility::getTopics)
                .flatMap(List::stream)
                .map(topic -> listAlterConfigOpsByConfigResource(topic, clusterConfiguration))
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public Topic createTopic(final String topicName, final Collection<String> principals, final TopicConfiguration topicConfiguration) {
        final Map<String, String> config = topicConfiguration.getConfig().entrySet().stream()
                .collect(Collectors.toMap(ConfigEntryUtils::getCamelCase, Map.Entry::getValue));
        final Topic topic = new Topic(topicName, topicConfiguration.getPartitions().size(), topicConfiguration.getReplicationFactor(), config);
        topic.getConsumers().addAll(principals.stream().map(AccessControl::new).collect(Collectors.toUnmodifiableList()));

        return topic;
    }

    @Override
    public void createTopics(final Collection<NewTopic> newTopics) throws ExecutionException, InterruptedException {
        kafkaClusterRepository.createTopics(newTopics);
    }

    @Override
    public void deleteTopics(final Collection<String> topicNames) throws ExecutionException, InterruptedException {
        kafkaClusterRepository.deleteTopics(topicNames);
    }

    @Override
    public Map<String, NewPartitions> createNewPartitions(final Collection<Domain> domains) throws ExecutionException, InterruptedException {
        final ClusterConfiguration clusterConfiguration = kafkaClusterRepository.getClusterConfiguration();
        return domains.stream()
                .map(Domain::getVisibilities)
                .flatMap(List::stream)
                .map(Visibility::getTopics)
                .flatMap(List::stream)
                .map(topic -> getNewPartitions(topic, clusterConfiguration))
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public Set<NewTopic> createNewTopics(final Collection<Domain> domains) throws ExecutionException, InterruptedException {
        final Collection<String> topicNames = kafkaClusterRepository.getClusterConfiguration().listTopicNames();
        return domains.stream()
                .map(Domain::getVisibilities)
                .flatMap(List::stream)
                .map(Visibility::getTopics)
                .flatMap(List::stream)
                .filter(topic -> !isTopicAvailable(topic, topicNames))
                .map(this::createNewTopic)
                .collect(Collectors.toSet());
    }

    @Override
    public void createPartitions(final Map<String, NewPartitions> newPartitions) throws ExecutionException, InterruptedException {
        kafkaClusterRepository.createPartitions(newPartitions);
    }

    @Override
    public Set<String> listOrphanedTopics(final Collection<Domain> domains) throws ExecutionException, InterruptedException {
        final ClusterConfiguration clusterConfiguration = kafkaClusterRepository.getClusterConfiguration();
        final Collection<String> clusterTopicNames = clusterConfiguration.listTopicNames();
        final Set<String> topologyTopicNames = listTopicNames(domains);

        return clusterTopicNames.stream()
                .filter(clusterTopicName -> isOrphanedTopic(clusterTopicName, topologyTopicNames))
                .collect(Collectors.toSet());
    }

    @Override
    public Collection<TopicConfiguration> listTopicsInCluster() throws ExecutionException, InterruptedException {
        final ClusterConfiguration clusterConfiguration = kafkaClusterRepository.getClusterConfiguration();
        if (Objects.isNull(clusterConfiguration)) {
            return Collections.emptyList();
        }

        return Collections.unmodifiableCollection(clusterConfiguration.getTopics());
    }

    public Set<String> listTopicNames(final Collection<Domain> domains) {
        return domains.stream()
                .map(Domain::getVisibilities)
                .flatMap(List::stream)
                .map(Visibility::getTopics)
                .flatMap(List::stream)
                .map(Topic::getFullName)
                .collect(Collectors.toSet());
    }

    @Override
    public void updateConfigs(final Map<ConfigResource, Collection<AlterConfigOp>> configs) throws ExecutionException, InterruptedException {
        kafkaClusterRepository.updateConfigs(configs);
    }

    public NewTopic createNewTopic(final Topic topic) {
        final Map<String, String> config = topic.getConfig().entrySet().stream()
                .collect(Collectors.toMap(ConfigEntryUtils::getDottedLowerCase, Map.Entry::getValue));

        return new NewTopic(topic.getFullName(), topic.getNumPartitions(), topic.getReplicationFactor())
                .configs(config);
    }

    public Map.Entry<String, NewPartitions> getNewPartitions(final Topic topic, final ClusterConfiguration clusterConfiguration) {
        final TopicConfiguration topicConfiguration = clusterConfiguration.getTopics().stream()
                .filter(t -> Objects.equals(t.getName(), topic.getFullName()))
                .findFirst()
                .orElse(null);
        if (Objects.nonNull(topicConfiguration) && topic.getNumPartitions() > topicConfiguration.getPartitions().size()) {
            return Map.entry(topic.getFullName(), NewPartitions.increaseTo(topic.getNumPartitions()));
        } else if (Objects.nonNull(topicConfiguration) && topic.getNumPartitions() < topicConfiguration.getPartitions().size()) {
            throw new IllegalArgumentException("Topic '" + topic.getFullName() + "' has smaller partitions size than in cluster.");
        } else {
            return null;
        }
    }

    public boolean isOrphanedTopic(String clusterTopicName, Set<String> topologyTopicNames) {
        return PATTERN_FULL_TOPIC_NAME.matcher(clusterTopicName).matches()
                && !topologyTopicNames.contains(clusterTopicName)
                && hasNoActiveConsumerGroups(clusterTopicName);
    }

    public boolean isTopicAvailable(final Topic topic, final Collection<String> existingTopicNames) {
        return existingTopicNames.stream()
                .anyMatch(item -> item.equals(topic.getFullName()));
    }

    public boolean hasNoActiveConsumerGroups(final String topicName) {
        try {
            final Collection<ConsumerGroupConfiguration> activeConsumerGroups = listActiveConsumerGroups(topicName);
            final boolean result = activeConsumerGroups.isEmpty();
            if (!result) {
                LOGGER.warn("Topic '{}' has active consumer groups: {}", topicName, activeConsumerGroups.stream()
                        .map(ConsumerGroupConfiguration::getGroupId)
                        .map(groupId -> groupId.isBlank() ? "default" : groupId)
                        .collect(Collectors.toList())
                );
            } else {
                LOGGER.info("Topic '{}' has no active consumer groups.", topicName);
            }

            return result;
        } catch (ExecutionException | InterruptedException e) {
            throw new IllegalStateException(e);
        }

    }

    public Collection<ConsumerGroupConfiguration> listActiveConsumerGroups(final String topicName) throws ExecutionException, InterruptedException {
        return kafkaClusterRepository.getClusterConfiguration().getConsumerGroups().stream()
                .filter(cg -> cg.getConsumers().stream().anyMatch(c -> Objects.equals(c.getPartition().getTopicName(), topicName)))
                .filter(cg -> !Objects.equals(ConsumerGroupConfiguration.State.DEAD, cg.getState()))
                .collect(Collectors.toList());
    }

    public Map.Entry<ConfigResource, Collection<AlterConfigOp>> listAlterConfigOpsByConfigResource(final Topic topic, final ClusterConfiguration clusterConfiguration) {
        final TopicConfiguration topicConfiguration = clusterConfiguration.getTopics().stream()
                .filter(t -> Objects.equals(t.getName(), topic.getFullName()))
                .findFirst()
                .orElse(null);
        if (Objects.isNull(topicConfiguration)) {
            return null;
        }
        final ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic.getFullName());
        final Set<AlterConfigOp> alterConfigOps = listConfigsToSet(topic, topicConfiguration);
        alterConfigOps.addAll(listConfigsToDelete(topic, topicConfiguration));
        if (alterConfigOps.isEmpty()) {
            return null;
        }

        return Map.entry(configResource, alterConfigOps);
    }

    public Set<AlterConfigOp> listConfigsToDelete(final Topic topic, final TopicConfiguration topicConfiguration) {
        final Set<AlterConfigOp> alterConfigOps = new HashSet<>();
        final Map<String, String> config = topicConfiguration.getConfig();
        for (final Map.Entry<String, String> entry : config.entrySet()) {
            final String key = entry.getKey();
            if (!topic.getConfig().containsKey(key)) {
                final String value = entry.getValue();
                final ConfigEntry configEntry = new ConfigEntry(ConfigEntryUtils.getDottedLowerCase(entry), value);
                final AlterConfigOp alterConfigOp = new AlterConfigOp(configEntry, AlterConfigOp.OpType.DELETE);
                alterConfigOps.add(alterConfigOp);
            }
        }

        return alterConfigOps;
    }

    public Set<AlterConfigOp> listConfigsToSet(final Topic topic, final TopicConfiguration topicConfiguration) {
        final Set<AlterConfigOp> alterConfigOps = new HashSet<>();
        final Map<String, String> config = topic.getConfig();
        for (final Map.Entry<String, String> entry : config.entrySet()) {
            final String key = entry.getKey();
            final String newValue = config.get(key);
            final String originalValue = topicConfiguration.getConfig().get(key);
            if (!Objects.equals(newValue, originalValue)) {
                final ConfigEntry configEntry = new ConfigEntry(ConfigEntryUtils.getDottedLowerCase(entry), newValue);
                final AlterConfigOp alterConfigOp = new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET);
                alterConfigOps.add(alterConfigOp);
            }
        }

        return alterConfigOps;
    }

}
