package de.volkerfaas.kafka.topology.services;

import de.volkerfaas.kafka.topology.model.TopologyFile;

import java.io.IOException;
import java.util.Set;

public interface DocumentationService {

    void writeReadmeFile(Set<TopologyFile> topologies) throws IOException;

}
