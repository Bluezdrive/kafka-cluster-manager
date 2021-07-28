package de.volkerfaas.kafka.topology.services;

import java.util.Collection;

public interface TopologyDeleteService {
    void deleteTopology(String directory, final Collection<String> domainNames);
}
