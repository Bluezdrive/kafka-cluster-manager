package de.volkerfaas.kafka.topology.services;

import de.volkerfaas.kafka.topology.model.Domain;
import de.volkerfaas.kafka.topology.model.Schema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.util.Collection;

public interface SchemaFileService {

    void downloadSchemas(final Collection<Schema> schemas, final String directory);
    Schema findSchema(Collection<String> subjects, String domainName, String fullTopicName, String suffix) throws IOException, RestClientException;
    Collection<String> listSubjects() throws IOException, RestClientException;
    void registerSchemas(Collection<Schema> schemaFiles, String directory);
    Collection<Schema> listSchemasByDomains(Collection<Domain> domains);

}
