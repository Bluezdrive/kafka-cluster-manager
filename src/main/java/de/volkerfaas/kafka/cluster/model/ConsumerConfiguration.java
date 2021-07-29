package de.volkerfaas.kafka.cluster.model;

public class ConsumerConfiguration {

    private final PartitionConfiguration partition;
    private final long offset;

    public ConsumerConfiguration(PartitionConfiguration partition, long offset) {
        this.partition = partition;
        this.offset = offset;
    }

    public PartitionConfiguration getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return "ConsumerConfiguration{" +
                "partition=" + partition +
                ", offset=" + offset +
                '}';
    }

}
