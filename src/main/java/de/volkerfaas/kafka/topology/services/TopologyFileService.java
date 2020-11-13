package de.volkerfaas.kafka.topology.services;

import de.volkerfaas.kafka.topology.model.TopologyFile;

import javax.validation.Valid;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public interface TopologyFileService {

    Set<TopologyFile> listTopologies(String directory, List<String> domainNames);
    Set<TopologyFile> restoreTopologies(String pathname, List<String> domainNames) throws ExecutionException, InterruptedException;
    void updateTopology(Set<TopologyFile> topologies) throws ExecutionException, InterruptedException, IOException;
    boolean isTopologyValid(Set<@Valid TopologyFile> topologies) throws ExecutionException, InterruptedException;

}
