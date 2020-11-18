package de.volkerfaas.kafka.topology.services;

import de.volkerfaas.kafka.topology.model.TopologyFile;

import javax.validation.Valid;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

public interface TopologyFileService {

    void deleteOrphanedAclBindings(Collection<TopologyFile> topologies) throws ExecutionException, InterruptedException;
    void deleteOrphanedTopics(Collection<TopologyFile> topologies) throws ExecutionException, InterruptedException;
    Collection<TopologyFile> listTopologies(String directory, List<String> domainNames);
    Collection<TopologyFile> restoreTopologies(String pathname, List<String> domainNames) throws ExecutionException, InterruptedException;
    void skipTopicsNotInEnvironment(Collection<TopologyFile> topologies, String environment);
    void updateTopology(Collection<TopologyFile> topologies) throws ExecutionException, InterruptedException, IOException;
    boolean isTopologyValid(Collection<@Valid TopologyFile> topologies) throws ExecutionException, InterruptedException;

}
