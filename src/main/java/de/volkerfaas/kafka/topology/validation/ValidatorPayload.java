package de.volkerfaas.kafka.topology.validation;

import de.volkerfaas.kafka.cluster.model.TopicConfiguration;
import de.volkerfaas.kafka.topology.model.TopologyFile;

import java.util.Collection;

public class ValidatorPayload {

    private final Collection<TopologyFile> topologies;
    private final Collection<TopicConfiguration> topicConfigurations;

    public ValidatorPayload(Collection<TopologyFile> topologies, Collection<TopicConfiguration> topicConfigurations) {
        this.topologies = topologies;
        this.topicConfigurations = topicConfigurations;
    }

    public Collection<TopologyFile> getTopologies() {
        return topologies;
    }

    public Collection<TopicConfiguration> getKafkaTopics() {
        return topicConfigurations;
    }
}
