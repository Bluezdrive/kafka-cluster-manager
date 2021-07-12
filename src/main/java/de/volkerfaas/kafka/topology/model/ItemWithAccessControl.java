package de.volkerfaas.kafka.topology.model;

import java.util.List;

public interface ItemWithAccessControl extends Item {

    List<AccessControl> getConsumers();
    List<AccessControl> getProducers();

}
