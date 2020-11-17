package de.volkerfaas.kafka.cluster.model;

import org.apache.kafka.common.acl.AclBinding;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class ClusterConfiguration {

    private final Collection<AclBinding> aclBindings;
    private final String clusterId;
    private final Collection<TopicConfiguration> topics;
    private final Collection<ConsumerGroupConfiguration> consumerGroups;

    public ClusterConfiguration(String clusterId) {
        this(clusterId, new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    }

    public ClusterConfiguration(String clusterId, List<TopicConfiguration> topics, List<AclBinding> aclBindings, Collection<ConsumerGroupConfiguration> consumerGroups) {
        this.aclBindings = aclBindings;
        this.clusterId = clusterId;
        this.topics = topics;
        this.consumerGroups = consumerGroups;
    }

    public Collection<AclBinding> getAclBindings() {
        return aclBindings;
    }

    public String getClusterId() {
        return clusterId;
    }

    public Collection<TopicConfiguration> getTopics() {
        return topics;
    }

    public Collection<ConsumerGroupConfiguration> getConsumerGroups() {
        return consumerGroups;
    }

    public Collection<String> listTopicNames() {
        return topics.stream()
                .map(TopicConfiguration::getName)
                .collect(Collectors.toSet());
    }

    @Override
    public String toString() {
        return "ClusterConfiguration{" +
                "aclBindings=" + aclBindings +
                ", clusterId=" + clusterId +
                ", topics=" + topics +
                '}';
    }
}
