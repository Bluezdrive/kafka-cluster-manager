package de.volkerfaas.kafka.topology.services;

import de.volkerfaas.kafka.topology.model.Domain;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface SchemaFileService {

    String downloadSchemaFile(String schemaFile);
    String findSchema(Collection<String> subjects, String domainName, String fullTopicName, String suffix);
    Collection<String> listSubjects();
    void registerSchemaFiles(Set<Path> schemaFiles);
    Set<Path> updateSchemaFiles(List<Domain> domains);

}
