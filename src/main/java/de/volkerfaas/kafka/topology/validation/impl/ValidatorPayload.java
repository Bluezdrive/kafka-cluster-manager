package de.volkerfaas.kafka.topology.validation.impl;

import de.volkerfaas.kafka.cluster.model.TopicConfiguration;
import de.volkerfaas.kafka.topology.model.TopologyFile;

import java.util.Collection;

public class ValidatorPayload {

    private final String directory;
    private final Collection<TopologyFile> topologies;
    private final Collection<TopicConfiguration> topicConfigurations;

    public ValidatorPayload(final String directory, final Collection<TopologyFile> topologies, final Collection<TopicConfiguration> topicConfigurations) {
        this.directory = directory;
        this.topologies = topologies;
        this.topicConfigurations = topicConfigurations;
    }

    public String getDirectory() {
        return directory;
    }

    public Collection<TopologyFile> getTopologies() {
        return topologies;
    }

    public Collection<TopicConfiguration> getTopicConfigurations() {
        return topicConfigurations;
    }
}
