package de.volkerfaas.kafka.topology.services;

import de.volkerfaas.kafka.topology.model.Domain;
import de.volkerfaas.kafka.topology.model.KafkaTopic;
import de.volkerfaas.kafka.topology.model.Topic;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public interface TopicService {

    Map<ConfigResource, Collection<AlterConfigOp>> createAlterConfigOperations(List<Domain> domains) throws ExecutionException, InterruptedException;
    Map<String, NewPartitions> createNewPartitions(List<Domain> domains) throws ExecutionException, InterruptedException;
    Set<NewTopic> createNewTopics(List<Domain> domains) throws ExecutionException, InterruptedException;
    void createPartitions(Map<String, NewPartitions> newPartitions) throws ExecutionException, InterruptedException;
    Topic createTopic(String topicName, KafkaTopic kafkaTopic);
    void createTopics(Set<NewTopic> newTopics) throws ExecutionException, InterruptedException;
    List<KafkaTopic> listTopicsInCluster() throws ExecutionException, InterruptedException;
    void updateConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs) throws ExecutionException, InterruptedException;

}
