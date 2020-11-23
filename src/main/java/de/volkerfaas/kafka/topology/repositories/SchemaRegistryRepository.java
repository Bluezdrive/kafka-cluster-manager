package de.volkerfaas.kafka.topology.repositories;

import de.volkerfaas.kafka.topology.model.Schema;

import java.util.Collection;

public interface SchemaRegistryRepository {

    String downloadSchema(Schema schema, String directory);
    String getCompatibilityMode(String subject);
    Collection<String> listSubjects();
    void registerSchemas(Collection<Schema> schemaFiles, String directory);

}
