package de.volkerfaas.kafka.cluster.model;

import java.util.List;
import java.util.Map;

public class TopicConfiguration {

    private final String name;
    private final List<PartitionConfiguration> partitions;
    private final short replicationFactor;
    private final Map<String, String> config;

    public TopicConfiguration(String name, List<PartitionConfiguration> partitions, short replicationFactor, Map<String, String> config) {
        this.name = name;
        this.partitions = partitions;
        this.replicationFactor = replicationFactor;
        this.config = config;
    }

    public String getName() {
        return name;
    }

    public List<PartitionConfiguration> getPartitions() {
        return partitions;
    }

    public short getReplicationFactor() {
        return replicationFactor;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    @Override
    public String toString() {
        return "TopicConfiguration{" +
                "name='" + name + '\'' +
                ", partitions=" + partitions +
                ", replicationFactor=" + replicationFactor +
                ", config=" + config +
                '}';
    }
}
