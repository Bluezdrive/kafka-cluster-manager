package de.volkerfaas.kafka.cluster.model;

public class PartitionConfiguration {

    private final String topicName;
    private final int index;
    private long offset;
    private long timestamp;

    public PartitionConfiguration(String topicName, int index) {
        this.topicName = topicName;
        this.index = index;
    }

    public String getTopicName() {
        return topicName;
    }

    public int getIndex() {
        return index;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "PartitionConfiguration{" +
                "topicName=" + topicName +
                ", index=" + index +
                ", topicOffset=" + offset +
                ", topicTimestamp=" + timestamp +
                '}';
    }

}
