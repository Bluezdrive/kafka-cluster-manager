package de.volkerfaas.kafka.topology.model;

import java.util.List;

public interface ConsumerAccessControl {

    List<AccessControl> getConsumers();
    String getFullName();

}
