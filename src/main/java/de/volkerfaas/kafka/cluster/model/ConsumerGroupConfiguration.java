package de.volkerfaas.kafka.cluster.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class ConsumerGroupConfiguration {

    public enum State {

        UNKNOWN("Unknown"),
        PREPARING_REBALANCE("PreparingRebalance"),
        COMPLETING_REBALANCE("CompletingRebalance"),
        STABLE("Stable"),
        DEAD("Dead"),
        EMPTY("Empty");

        private final String value;

        State(String value) {
            this.value = value;
        }

        public static State findByValue(String value) {
            return Arrays.stream(State.values())
                    .filter(v -> Objects.equals(v.getValue(), value))
                    .findFirst()
                    .orElse(UNKNOWN);
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return value;
        }

    }

    private final String groupId;
    private final State state;
    private final List<ConsumerConfiguration> consumers;

    public ConsumerGroupConfiguration(String groupId, State state) {
        this.groupId = groupId;
        this.state = state;
        this.consumers = new ArrayList<>();
    }

    public String getGroupId() {
        return groupId;
    }

    public State getState() {
        return state;
    }

    public List<ConsumerConfiguration> getConsumers() {
        return consumers;
    }

    @Override
    public String toString() {
        return "ConsumerGroupConfiguration{" +
                "groupId='" + groupId + '\'' +
                ", state=" + state +
                ", consumers=" + consumers +
                '}';
    }
}
