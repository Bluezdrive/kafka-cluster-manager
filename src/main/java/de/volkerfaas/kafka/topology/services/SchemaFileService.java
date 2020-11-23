package de.volkerfaas.kafka.topology.services;

import de.volkerfaas.kafka.topology.model.Domain;
import de.volkerfaas.kafka.topology.model.Schema;

import java.util.Collection;

public interface SchemaFileService {

    void downloadSchemas(final Collection<Schema> schemas, final String directory);
    Schema findSchema(Collection<String> subjects, String domainName, String fullTopicName, String suffix);
    Collection<String> listSubjects();
    void registerSchemas(Collection<Schema> schemaFiles, String directory);
    Collection<Schema> listSchemasByDomains(Collection<Domain> domains);

}
