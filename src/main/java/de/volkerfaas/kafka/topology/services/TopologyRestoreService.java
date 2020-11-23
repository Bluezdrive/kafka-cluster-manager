package de.volkerfaas.kafka.topology.services;

import de.volkerfaas.kafka.topology.model.TopologyFile;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

public interface TopologyRestoreService {

    Collection<TopologyFile> restoreTopologies(String pathname, List<String> domainNames) throws ExecutionException, InterruptedException;

}
