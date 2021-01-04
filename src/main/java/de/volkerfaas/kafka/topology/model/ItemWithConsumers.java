package de.volkerfaas.kafka.topology.model;

import java.util.List;

public interface ItemWithConsumers extends Item {

    List<AccessControl> getConsumers();

}
