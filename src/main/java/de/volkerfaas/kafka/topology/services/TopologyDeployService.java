package de.volkerfaas.kafka.topology.services;

import de.volkerfaas.kafka.topology.model.Domain;
import de.volkerfaas.kafka.topology.model.TopologyFile;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import javax.validation.Valid;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

public interface TopologyDeployService {

    void deleteOrphanedAclBindings(Collection<Domain> domains) throws ExecutionException, InterruptedException;
    void deleteOrphanedSubjects(Collection<Domain> domains) throws IOException, RestClientException;
    void deleteOrphanedTopics(Collection<Domain> domains) throws ExecutionException, InterruptedException;
    boolean isTopologyValid(Collection<@Valid TopologyFile> topologies, String directory) throws ExecutionException, InterruptedException;
    Collection<TopologyFile> listTopologies(String directory);
    Collection<Domain> filterDomainsForUpdate(Collection<TopologyFile> topologies, Collection<String> domainNames);
    void removeTopicsNotInCluster(final Collection<TopologyFile> topologies, String cluster);
    void updateTopology(Collection<Domain> domains, String directory) throws ExecutionException, InterruptedException, IOException;

}
