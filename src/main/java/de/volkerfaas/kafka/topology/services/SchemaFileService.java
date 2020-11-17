package de.volkerfaas.kafka.topology.services;

import de.volkerfaas.kafka.topology.model.Domain;

import java.nio.file.Path;
import java.util.Collection;

public interface SchemaFileService {

    String downloadSchemaFile(String schemaFile);
    String findSchema(Collection<String> subjects, String domainName, String fullTopicName, String suffix);
    Collection<String> listSubjects();
    void registerSchemaFiles(Collection<Path> schemaFiles);
    Collection<Path> listSchemaFilesByDomains(Collection<Domain> domains);

}
