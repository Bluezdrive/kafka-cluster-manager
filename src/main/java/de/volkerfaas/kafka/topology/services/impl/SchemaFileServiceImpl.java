package de.volkerfaas.kafka.topology.services.impl;

import de.volkerfaas.kafka.topology.ApplicationConfiguration;
import de.volkerfaas.kafka.topology.model.Domain;
import de.volkerfaas.kafka.topology.model.Schema;
import de.volkerfaas.kafka.topology.model.Topic;
import de.volkerfaas.kafka.topology.model.Visibility;
import de.volkerfaas.kafka.topology.repositories.SchemaRegistryRepository;
import de.volkerfaas.kafka.topology.services.SchemaFileService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class SchemaFileServiceImpl implements SchemaFileService {

    private final SchemaRegistryRepository schemaRegistryRepository;

    @Autowired
    public SchemaFileServiceImpl(final SchemaRegistryRepository schemaRegistryRepository) {
        this.schemaRegistryRepository = schemaRegistryRepository;
    }

    @Override
    public void downloadSchemas(final Collection<Schema> schemas, final String directory) {
        schemas.forEach(schema -> schemaRegistryRepository.downloadSchema(schema, directory));
    }

    @Override
    public Schema findSchema(final Collection<String> subjects, final String domainName, final String fullTopicName, final String suffix) {
        final String subject = subjects.stream()
                .filter(s -> Objects.equals(s, fullTopicName + suffix))
                .findFirst()
                .orElse(null);
        if (Objects.isNull(subject)) {
            return null;
        }

        final String schemaFileName = Path.of(ApplicationConfiguration.EVENTS_DIRECTORY, domainName, subject + ".avsc").toString();

        final String compatibility = getCompatibility(subject);

        return new Schema(schemaFileName, null, Schema.CompatibilityMode.valueOf(compatibility));
    }

    public String getCompatibility(final String subject) {
        return schemaRegistryRepository.getCompatibilityMode(subject);
    }

    @Override
    public Collection<String> listSubjects() {
        return schemaRegistryRepository.listSubjects();
    }

    @Override
    public void registerSchemas(final Collection<Schema> schemas, final String directory) {
        schemaRegistryRepository.registerSchemas(schemas, directory);
    }

    @Override
    public Set<Schema> listSchemasByDomains(final Collection<Domain> domains) {
        return domains.stream()
                .map(Domain::getVisibilities)
                .flatMap(List::stream)
                .map(Visibility::getTopics)
                .flatMap(List::stream)
                .map(this::listSchemaFilesByTopic)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    public Set<Schema> listSchemaFilesByTopic(final Topic topic) {
        final Set<Schema> schemaFiles = new HashSet<>();
        final Schema keySchema = topic.getKeySchema();
        if (Objects.nonNull(keySchema) && StringUtils.hasText(keySchema.getFile())) {
            schemaFiles.add(keySchema);
        }
        final Schema valueSchema = topic.getValueSchema();
        if (Objects.nonNull(valueSchema) && StringUtils.hasText(valueSchema.getFile())) {
            schemaFiles.add(valueSchema);
        }

        return schemaFiles;
    }

}
