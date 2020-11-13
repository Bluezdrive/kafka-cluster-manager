package de.volkerfaas.kafka.topology.repositories;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Set;

public interface SchemaRegistryRepository {

    String downloadSchemaFile(String schemaFile);
    String getSchema(String subject);
    Collection<String> listSubjects();
    void registerSchemaFiles(Set<Path> schemaFiles);

}
