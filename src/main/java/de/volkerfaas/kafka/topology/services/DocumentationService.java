package de.volkerfaas.kafka.topology.services;

import de.volkerfaas.kafka.topology.model.TopologyFile;

import java.io.IOException;
import java.util.Collection;

public interface DocumentationService {

    void writeTopologyDocumentationFile(Collection<TopologyFile> topologies, String directory) throws IOException;

    void writeEventsDocumentationFile(Collection<TopologyFile> topologies, String directory) throws IOException;
}
