package de.volkerfaas.kafka.topology.repositories;

import de.volkerfaas.kafka.topology.model.TopologyFile;

import java.util.Set;

public interface TopologyFileRepository {

    Set<String> listTopologyFiles(String directory);
    TopologyFile readTopology(String pathname);
    TopologyFile writeTopology(TopologyFile topology);

}
