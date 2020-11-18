package de.volkerfaas.kafka.topology.services;

import de.volkerfaas.kafka.topology.model.TopologyFile;

import java.io.IOException;
import java.util.Collection;

public interface DocumentationService {

    void writeReadmeFile(Collection<TopologyFile> topologies) throws IOException;

}
