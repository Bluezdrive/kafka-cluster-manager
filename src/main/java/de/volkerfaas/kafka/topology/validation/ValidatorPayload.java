package de.volkerfaas.kafka.topology.validation;

import de.volkerfaas.kafka.topology.model.KafkaTopic;
import de.volkerfaas.kafka.topology.model.TopologyFile;

import java.util.List;
import java.util.Set;

public class ValidatorPayload {

    private final Set<TopologyFile> topologies;
    private final List<KafkaTopic> kafkaTopics;

    public ValidatorPayload(Set<TopologyFile> topologies, List<KafkaTopic> kafkaTopics) {
        this.topologies = topologies;
        this.kafkaTopics = kafkaTopics;
    }

    public Set<TopologyFile> getTopologies() {
        return topologies;
    }

    public List<KafkaTopic> getKafkaTopics() {
        return kafkaTopics;
    }
}
