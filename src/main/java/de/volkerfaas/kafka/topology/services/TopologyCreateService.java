package de.volkerfaas.kafka.topology.services;

import com.fasterxml.jackson.core.JsonProcessingException;

public interface TopologyCreateService {
    void createTopology(String directory, String domainName, final String description, final String maintainerName, final String maintainerEmail, final String serviceAccountId) throws JsonProcessingException;
}
