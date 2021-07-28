package de.volkerfaas.kafka.topology.services;

import de.volkerfaas.kafka.topology.model.TopologyFile;

public interface TopologyValuesService {

    TopologyFile addAdditionalValues(final TopologyFile topology);

}
