package de.volkerfaas.kafka.topology.services.impl;

import de.volkerfaas.kafka.topology.model.*;
import de.volkerfaas.kafka.topology.repositories.KafkaClusterRepository;
import de.volkerfaas.kafka.topology.services.TopicService;
import de.volkerfaas.kafka.topology.utils.ConfigEntries;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
public class TopicServiceImpl implements TopicService {

    private final KafkaClusterRepository kafkaClusterRepository;

    @Autowired
    public TopicServiceImpl(KafkaClusterRepository kafkaClusterRepository) {
        this.kafkaClusterRepository = kafkaClusterRepository;
    }

    @Override
    public Map<ConfigResource, Collection<AlterConfigOp>> createAlterConfigOperations(List<Domain> domains) throws ExecutionException, InterruptedException {
        final KafkaCluster kafkaCluster = kafkaClusterRepository.dumpCluster();
        return domains.stream()
                .map(Domain::getVisibilities)
                .flatMap(List::stream)
                .map(Visibility::getTopics)
                .flatMap(List::stream)
                .map(topic -> listAlterConfigOpsByConfigResource(topic, kafkaCluster))
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public Topic createTopic(String topicName, KafkaTopic kafkaTopic) {
        final Map<String, String> config = kafkaTopic.getConfig().entrySet().stream()
                .collect(Collectors.toMap(ConfigEntries::getCamelCase, Map.Entry::getValue));
        return new Topic(topicName, kafkaTopic.getNumPartitions(), kafkaTopic.getReplicationFactor(), config);
    }

    @Override
    public void createTopics(Set<NewTopic> newTopics) throws ExecutionException, InterruptedException {
        kafkaClusterRepository.createTopics(newTopics);
    }

    @Override
    public Map<String, NewPartitions> createNewPartitions(List<Domain> domains) throws ExecutionException, InterruptedException {
        final KafkaCluster kafkaCluster = kafkaClusterRepository.dumpCluster();
        return domains.stream()
                .map(Domain::getVisibilities)
                .flatMap(List::stream)
                .map(Visibility::getTopics)
                .flatMap(List::stream)
                .map(topic -> getNewPartitions(topic, kafkaCluster))
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public Set<NewTopic> createNewTopics(List<Domain> domains) throws ExecutionException, InterruptedException {
        final Set<String> topicNames = kafkaClusterRepository.dumpCluster().getTopicNames();
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
    public void createPartitions(Map<String, NewPartitions> newPartitions) throws ExecutionException, InterruptedException {
        kafkaClusterRepository.createPartitions(newPartitions);
    }

    @Override
    public List<KafkaTopic> listTopicsInCluster() throws ExecutionException, InterruptedException {
        final KafkaCluster kafkaCluster = kafkaClusterRepository.dumpCluster();
        if (Objects.isNull(kafkaCluster)) {
            return Collections.emptyList();
        }

        return Collections.unmodifiableList(kafkaCluster.getTopics());
    }

    @Override
    public void updateConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs) throws ExecutionException, InterruptedException {
        kafkaClusterRepository.updateConfigs(configs);
    }

    NewTopic createNewTopic(Topic topic) {
        final Map<String, String> config = topic.getConfig().entrySet().stream()
                .collect(Collectors.toMap(ConfigEntries::getDottedLowerCase, Map.Entry::getValue));

        return new NewTopic(topic.getFullName(), topic.getNumPartitions(), topic.getReplicationFactor())
                .configs(config);
    }

    Map.Entry<String, NewPartitions> getNewPartitions(Topic topic, KafkaCluster kafkaCluster) {
        final KafkaTopic kafkaTopic = kafkaCluster.getTopics().stream()
                .filter(t -> Objects.equals(t.getName(), topic.getFullName()))
                .findFirst()
                .orElse(null);
        if (Objects.nonNull(kafkaTopic) && topic.getNumPartitions() > kafkaTopic.getNumPartitions()) {
            return Map.entry(topic.getFullName(), NewPartitions.increaseTo(topic.getNumPartitions()));
        } else if (Objects.nonNull(kafkaTopic) && topic.getNumPartitions() < kafkaTopic.getNumPartitions()) {
            throw new IllegalArgumentException("Topic '" + topic.getFullName() + "' has smaller partitions size than in cluster.");
        } else {
            return null;
        }
    }

    Map.Entry<ConfigResource, Collection<AlterConfigOp>> listAlterConfigOpsByConfigResource(Topic topic, KafkaCluster kafkaCluster) {
        final KafkaTopic kafkaTopic = kafkaCluster.getTopics().stream()
                .filter(t -> Objects.equals(t.getName(), topic.getFullName()))
                .findFirst()
                .orElse(null);
        if (Objects.isNull(kafkaTopic)) {
            return null;
        }
        final ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic.getFullName());
        final Set<AlterConfigOp> alterConfigOps = listConfigsToSet(topic, kafkaTopic);
        alterConfigOps.addAll(listConfigsToDelete(topic, kafkaTopic));
        if (alterConfigOps.isEmpty()) {
            return null;
        }

        return Map.entry(configResource, alterConfigOps);
    }

    Set<AlterConfigOp> listConfigsToDelete(Topic topic, KafkaTopic kafkaTopic) {
        final Set<AlterConfigOp> alterConfigOps = new HashSet<>();
        final Map<String, String> config = kafkaTopic.getConfig();
        for (Map.Entry<String, String> entry : config.entrySet()) {
            final String key = entry.getKey();
            if (!topic.getConfig().containsKey(key)) {
                final String value = entry.getValue();
                ConfigEntry configEntry = new ConfigEntry(ConfigEntries.getDottedLowerCase(entry), value);
                AlterConfigOp alterConfigOp = new AlterConfigOp(configEntry, AlterConfigOp.OpType.DELETE);
                alterConfigOps.add(alterConfigOp);
            }
        }

        return alterConfigOps;
    }

    Set<AlterConfigOp> listConfigsToSet(Topic topic, KafkaTopic kafkaTopic) {
        final Set<AlterConfigOp> alterConfigOps = new HashSet<>();
        final Map<String, String> config = topic.getConfig();
        for (Map.Entry<String, String> entry : config.entrySet()) {
            final String key = entry.getKey();
            final String newValue = config.get(key);
            final String originalValue = kafkaTopic.getConfig().get(key);
            if (!Objects.equals(newValue, originalValue)) {
                ConfigEntry configEntry = new ConfigEntry(ConfigEntries.getDottedLowerCase(entry), newValue);
                AlterConfigOp alterConfigOp = new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET);
                alterConfigOps.add(alterConfigOp);
            }
        }

        return alterConfigOps;
    }

    boolean isTopicAvailable(Topic topic, Set<String> existingTopicNames) {
        return existingTopicNames.stream()
                .anyMatch(item -> item.equals(topic.getFullName()));
    }

}
