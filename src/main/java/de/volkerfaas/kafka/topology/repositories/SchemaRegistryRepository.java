package de.volkerfaas.kafka.topology.repositories;

import de.volkerfaas.kafka.topology.model.Schema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.util.Collection;

public interface SchemaRegistryRepository {

    void downloadSchemas(final Collection<Schema> schemas, final String directory);
    String getCompatibilityMode(String subject) throws IOException, RestClientException;
    String getSchemaType(String subject) throws IOException, RestClientException;
    Collection<String> listSubjects() throws IOException, RestClientException;
    void registerSchemas(Collection<Schema> schemaFiles, String directory);

}
