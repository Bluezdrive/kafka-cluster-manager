package de.volkerfaas.kafka.topology.services;

import de.volkerfaas.kafka.cluster.model.TopicConfiguration;
import de.volkerfaas.kafka.topology.model.Domain;
import de.volkerfaas.kafka.topology.model.Topic;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public interface TopicService {

    Map<ConfigResource, Collection<AlterConfigOp>> createAlterConfigOperations(Collection<Domain> domains) throws ExecutionException, InterruptedException;
    Map<String, NewPartitions> createNewPartitions(Collection<Domain> domains) throws ExecutionException, InterruptedException;
    Collection<NewTopic> createNewTopics(Collection<Domain> domains) throws ExecutionException, InterruptedException;
    void createPartitions(Map<String, NewPartitions> newPartitions) throws ExecutionException, InterruptedException;
    Topic createTopic(String topicName, final Collection<String> principals, TopicConfiguration topicConfiguration);
    void createTopics(Collection<NewTopic> newTopics) throws ExecutionException, InterruptedException;
    void deleteTopics(Collection<String> topicNames) throws ExecutionException, InterruptedException;
    Collection<String> listOrphanedTopics(Collection<Domain> domains) throws ExecutionException, InterruptedException;
    Collection<TopicConfiguration> listTopicsInCluster() throws ExecutionException, InterruptedException;

    Set<String> listTopicNames(Collection<Domain> domains);

    void updateConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs) throws ExecutionException, InterruptedException;

}
