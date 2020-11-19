package de.volkerfaas.kafka.topology.repositories.impl;

import de.volkerfaas.kafka.topology.ApplicationConfiguration;
import de.volkerfaas.kafka.topology.repositories.SchemaRegistryRepository;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Repository
public class SchemaRegistryRepositoryImpl implements SchemaRegistryRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaRegistryRepositoryImpl.class);

    private final Environment environment;
    private final SchemaRegistryClient schemaRegistryClient;

    @Autowired
    public SchemaRegistryRepositoryImpl(Environment environment, SchemaRegistryClient schemaRegistryClient) {
        this.environment = environment;
        this.schemaRegistryClient = schemaRegistryClient;
    }

    @Override
    public String getSchema(String subject) {
        try {
            final SchemaMetadata latestSchemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(subject);

            return latestSchemaMetadata.getSchema();
        } catch (RestClientException e) {
            if (e.getErrorCode() == 40401) {
                return null;
            } else {
                throw new IllegalStateException(e);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Collection<String> listSubjects() {
        try {
            return schemaRegistryClient.getAllSubjects();
        } catch (RestClientException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void registerSchemaFiles(Collection<Path> schemaFiles) {
        schemaFiles.forEach(this::registerSchemaFile);
    }

    @Override
    public String downloadSchemaFile(String schemaFile) {
        if (Objects.isNull(schemaFile) || schemaFile.isBlank()) {
            return null;
        }

        final Pattern pattern = Pattern.compile(ApplicationConfiguration.REGEX_SCHEMA_PATH_RELATIVE);
        final Matcher matcher = pattern.matcher(schemaFile);
        if (matcher.matches() && matcher.groupCount() == 6) {
            final String domainName = matcher.group(3);
            final String subject = matcher.group(2);
            final String schema = getSchema(subject);
            if (Objects.isNull(schema)) return null;
            final String topologyDirectory = environment.getRequiredProperty(ApplicationConfiguration.PROPERTY_KEY_TOPOLOGY_DIRECTORY);
            final Path schemaDirectoryPath = Path.of(topologyDirectory, ApplicationConfiguration.EVENTS_DIRECTORY, domainName);
            final Path schemaFilePath = Path.of(topologyDirectory, ApplicationConfiguration.EVENTS_DIRECTORY, domainName, subject + ".avsc");
            try {
                Files.createDirectories(schemaDirectoryPath);
                Files.writeString(schemaFilePath, schema, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

                LOGGER.info("Downloaded schema to {}", schemaFilePath);

                return schemaFilePath.toString();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        } else return null;
    }

    public String getContent(Path schemaFile) {
        try {
            final String schema = Files.readString(schemaFile);
            if (schema == null || schema.isEmpty() || schema.isBlank()) {
                throw new IllegalStateException("Schema '" + schemaFile + "' must not be empty!");
            }
            return schema;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public String getSchemaSubject(Path schemaFile) {
        final String fileName = schemaFile.toString();
        final Pattern pattern = Pattern.compile(ApplicationConfiguration.REGEX_SCHEMA_PATH_RELATIVE);
        final Matcher matcher = pattern.matcher(fileName);

        if (matcher.matches() && matcher.groupCount() > 2) {
            return matcher.group(2);
        } else {
            throw new IllegalArgumentException("Schema filename is not valid: " + schemaFile);
        }
    }

    public void registerSchemaFile(Path schemaFile) {
        final String subject = getSchemaSubject(schemaFile);
        final ParsedSchema parsedSchema = parseSchema(schemaFile);
        try {
            final boolean compatible = schemaRegistryClient.testCompatibility(subject, parsedSchema);
            if (!compatible) {
                LOGGER.error("Schema for subject '{}' is incompatible to existing schemas", subject);
                return;
            }

            final int version = getVersion(subject, parsedSchema);
            if (version > 0) {
                LOGGER.info("Schema for subject '{}' already exists and has version {}", subject, version);
                return;
            }

            if (isNotDryRun()) {
                final int schemaId = schemaRegistryClient.register(subject, parsedSchema);
                LOGGER.info("Schema updated for subject '{}' with schema ID {}", subject, schemaId);
            } else {
                LOGGER.info("Schema to be updated for subject '{}'", subject);
            }
        } catch (IOException | RestClientException e) {
            throw new IllegalStateException(e);
        }
    }

    public int getVersion(String subject, ParsedSchema parsedSchema) {
        try {
            final List<Integer> allVersions = schemaRegistryClient.getAllVersions(subject);
            if (allVersions.isEmpty()) {
                return 0;
            }
            return schemaRegistryClient.getVersion(subject, parsedSchema);
        } catch (RestClientException e) {
            if (e.getErrorCode() == 40401) {
                return 0;
            } else {
                throw new IllegalStateException(e);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public boolean isNotDryRun() {
        return !environment.getRequiredProperty(ApplicationConfiguration.PROPERTY_KEY_TOPOLOGY_DRY_RUN, boolean.class);
    }

    public ParsedSchema parseSchema(Path schemaFile) {
        if (schemaFile == null) throw new IllegalArgumentException("Path to schema file mot not be null.");

        final String schema = getContent(schemaFile);

        return schemaRegistryClient.parseSchema(AvroSchema.TYPE, schema, Collections.emptyList()).orElseThrow();
    }

}
