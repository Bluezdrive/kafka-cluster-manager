package de.volkerfaas.kafka.topology.model;

import org.apache.kafka.common.acl.AclBinding;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaCluster {

    private final String clusterId;
    private final List<AclBinding> aclBindings;
    private final List<KafkaTopic> topics;

    public KafkaCluster(String clusterId) {
        this.clusterId = clusterId;
        this.aclBindings = new ArrayList<>();
        this.topics = new ArrayList<>();
    }

    public List<AclBinding> getAclBindings() {
        return aclBindings;
    }

    public String getClusterId() {
        return clusterId;
    }

    public List<KafkaTopic> getTopics() {
        return topics;
    }

    public Set<String> getTopicNames() {
        return topics.stream().map(KafkaTopic::getName).collect(Collectors.toSet());
    }

    @Override
    public String toString() {
        return "{" +
                "topics=" + topics +
                '}';
    }
}
