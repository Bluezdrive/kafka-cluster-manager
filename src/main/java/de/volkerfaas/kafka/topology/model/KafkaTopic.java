package de.volkerfaas.kafka.topology.model;

import java.util.Map;

public class KafkaTopic {

    private final String name;
    private final int numPartitions;
    private final short replicationFactor;
    private final Map<String, String> config;

    public KafkaTopic(String name, int numPartitions, short replicationFactor, Map<String, String> config) {
        this.name = name;
        this.numPartitions = numPartitions;
        this.replicationFactor = replicationFactor;
        this.config = config;
    }

    public String getName() {
        return name;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public short getReplicationFactor() {
        return replicationFactor;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    @Override
    public String toString() {
        return "{" +
                "name='" + name + '\'' +
                ", numPartitions=" + numPartitions +
                ", replicationFactor=" + replicationFactor +
                ", config=" + config +
                '}';
    }
}
