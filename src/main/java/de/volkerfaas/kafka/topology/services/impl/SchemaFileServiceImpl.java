package de.volkerfaas.kafka.topology.services.impl;

import de.volkerfaas.kafka.topology.ApplicationConfiguration;
import de.volkerfaas.kafka.topology.model.Domain;
import de.volkerfaas.kafka.topology.model.Topic;
import de.volkerfaas.kafka.topology.model.Visibility;
import de.volkerfaas.kafka.topology.repositories.SchemaRegistryRepository;
import de.volkerfaas.kafka.topology.services.SchemaFileService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class SchemaFileServiceImpl implements SchemaFileService {

    private final Environment environment;
    private final SchemaRegistryRepository schemaRegistryRepository;

    @Autowired
    public SchemaFileServiceImpl(Environment environment, SchemaRegistryRepository schemaRegistryRepository) {
        this.environment = environment;
        this.schemaRegistryRepository = schemaRegistryRepository;
    }


    @Override
    public String downloadSchemaFile(String schemaFile) {
        return schemaRegistryRepository.downloadSchemaFile(schemaFile);
    }

    @Override
    public String findSchema(Collection<String> subjects, String domainName, String fullTopicName, String suffix) {
        final String subject = subjects.stream().filter(s -> Objects.equals(s, fullTopicName + suffix)).findFirst().orElse(null);
        if (Objects.isNull(subject)) {
            return null;
        }

        return Path.of(ApplicationConfiguration.EVENTS_DIRECTORY, domainName, subject + ".avsc").toString();
    }

    @Override
    public Collection<String> listSubjects() {
        return schemaRegistryRepository.listSubjects();
    }

    @Override
    public void registerSchemaFiles(Set<Path> schemaFiles) {
        schemaRegistryRepository.registerSchemaFiles(schemaFiles);
    }

    @Override
    public Set<Path> updateSchemaFiles(List<Domain> domains) {
        return domains.stream()
                .map(Domain::getVisibilities)
                .flatMap(List::stream)
                .map(Visibility::getTopics)
                .flatMap(List::stream)
                .map(this::addSchemaFiles)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    public Set<Path> addSchemaFiles(Topic topic) {
        Set<Path> schemaFiles = new HashSet<>();
        final String directory = environment.getRequiredProperty(ApplicationConfiguration.PROPERTY_KEY_TOPOLOGY_DIRECTORY);
        final String keySchemaFile = topic.getKeySchemaFile();
        if (StringUtils.hasText(keySchemaFile)) {
            schemaFiles.add(Path.of(directory, keySchemaFile));
        }
        final String valueSchemaFile = topic.getValueSchemaFile();
        if (StringUtils.hasText(valueSchemaFile)) {
            schemaFiles.add(Path.of(directory, valueSchemaFile));
        }

        return schemaFiles;
    }

}
