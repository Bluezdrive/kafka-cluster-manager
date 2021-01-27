package de.volkerfaas.kafka.topology.services.impl;

import de.volkerfaas.kafka.topology.model.Domain;
import de.volkerfaas.kafka.topology.model.Schema;
import de.volkerfaas.kafka.topology.model.Topic;
import de.volkerfaas.kafka.topology.model.Visibility;
import de.volkerfaas.kafka.topology.repositories.SchemaRegistryRepository;
import de.volkerfaas.kafka.topology.services.SchemaFileService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.IOException;
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
        schemaRegistryRepository.downloadSchemas(schemas, directory);
    }

    @Override
    public Schema findSchema(final Collection<String> subjects, final String domainName, final String fullTopicName, final String suffix) throws IOException, RestClientException {
        final String subject = subjects.stream()
                .filter(s -> Objects.equals(s, fullTopicName + suffix))
                .findFirst()
                .orElse(null);
        if (Objects.isNull(subject)) {
            return null;
        }
        final Schema.Type schemaType = getSchemaType(subject);
        final Schema.CompatibilityMode compatibilityMode = getCompatibility(subject);

        return new Schema(subject, schemaType, compatibilityMode);
    }

    @Override
    public Collection<String> listSubjects() throws IOException, RestClientException {
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

    public Schema.CompatibilityMode getCompatibility(final String subject) throws IOException, RestClientException {
        final String compatibilityMode = schemaRegistryRepository.getCompatibilityMode(subject);

        return Schema.CompatibilityMode.valueOf(compatibilityMode);
    }

    public Schema.Type getSchemaType(String subject) throws IOException, RestClientException {
        final String schemaType = schemaRegistryRepository.getSchemaType(subject);

        return Schema.Type.valueOf(schemaType);
    }

    public Set<Schema> listSchemaFilesByTopic(final Topic topic) {
        final Set<Schema> schemaFiles = new HashSet<>();
        final Schema keySchema = topic.getKeySchema();
        if (Objects.nonNull(keySchema) && StringUtils.hasText(keySchema.getSubject())) {
            schemaFiles.add(keySchema);
        }
        final Schema valueSchema = topic.getValueSchema();
        if (Objects.nonNull(valueSchema) && StringUtils.hasText(valueSchema.getSubject())) {
            schemaFiles.add(valueSchema);
        }

        return schemaFiles;
    }

}
