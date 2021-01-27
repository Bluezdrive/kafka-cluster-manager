package de.volkerfaas.kafka.topology.repositories.impl;

import com.github.freva.asciitable.Column;
import de.volkerfaas.kafka.topology.model.Schema;
import de.volkerfaas.kafka.topology.repositories.SchemaRegistryRepository;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static com.github.freva.asciitable.AsciiTable.getTable;
import static com.github.freva.asciitable.HorizontalAlign.LEFT;
import static de.volkerfaas.kafka.topology.utils.SchemaUtils.getSchemaPath;
import static de.volkerfaas.utils.ExceptionUtils.handleException;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

@Repository
public class SchemaRegistryRepositoryImpl implements SchemaRegistryRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaRegistryRepositoryImpl.class);

    private final boolean dryRun;
    private final SchemaRegistryClient schemaRegistryClient;

    @Autowired
    public SchemaRegistryRepositoryImpl(@Lazy final SchemaRegistryClient schemaRegistryClient, @Value("${dry-run:@null}") final String dryRun) {
        this.dryRun = Objects.nonNull(dryRun);
        this.schemaRegistryClient = schemaRegistryClient;
    }

    @Override
    public String getCompatibilityMode(final String subject) throws IOException, RestClientException {
        return getCompatibilityMode(subject, true);
    }

    public String getCompatibilityMode(final String subject, final boolean resolveDefault) throws IOException, RestClientException {
        try {
            return schemaRegistryClient.getCompatibility(subject);
        } catch (RestClientException e) {
            if ((e.getErrorCode() == 40401 || e.getErrorCode() == 40403) && resolveDefault) {
                return getCompatibilityMode(null, false);
            } else {
                throw e;
            }
        }
    }

    @Override
    public String getSchemaType(String subject) throws IOException, RestClientException {
        final SchemaMetadata latestSchemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(subject);

        return latestSchemaMetadata.getSchemaType();
    }

    @Override
    public Collection<String> listSubjects() throws IOException, RestClientException {
        return schemaRegistryClient.getAllSubjects();
    }

    @Override
    public void registerSchemas(final Collection<Schema> schemas, final String directory) {
        final Set<Schema> registeredSchemas = schemas.stream()
                .filter(Objects::nonNull)
                .peek(schema -> handleException(() -> updateCompatibility(schema)))
                .map(schema -> handleException(() -> registerSchema(schema, directory)))
                .filter(Objects::nonNull)
                .collect(Collectors.toUnmodifiableSet());
        printSchemas(registeredSchemas);
    }

    @Override
    public void downloadSchemas(final Collection<Schema> schemas, final String directory) {
        final Set<Schema> downloadedSchemas = schemas.stream()
                .map(schema -> handleException(() -> downloadSchema(schema, directory)))
                .filter(Objects::nonNull)
                .collect(Collectors.toUnmodifiableSet());
        printSchemas(downloadedSchemas);
    }

    public Schema downloadSchema(final Schema schema, final String directory) throws IOException, RestClientException {
        if (Objects.isNull(schema)) {
            LOGGER.error("No schema or schema file found");
            return null;
        }
        final String subject = schema.getSubject();
        try {
            final SchemaMetadata latestSchemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(subject);
            if (Objects.isNull(latestSchemaMetadata)) {
                LOGGER.error("No schema meta data available for subject '{}'", subject);
                return null;
            }
            final String schemaType = latestSchemaMetadata.getSchemaType();
            if (Objects.isNull(schemaType)) {
                LOGGER.error("No schema type specified for subject '{}'", subject);
                return null;
            }
            schema.setType(Schema.Type.valueOf(schemaType));
            final String compatibilityMode = getCompatibilityMode(subject);
            if (Objects.nonNull(compatibilityMode)) {
                schema.setCompatibilityMode(Schema.CompatibilityMode.valueOf(compatibilityMode));
            }
            final String schemaContent = latestSchemaMetadata.getSchema();
            if (Objects.isNull(schemaContent)) {
                LOGGER.error("No content available for subject '{}'", subject);
                return null;
            }
            final Path schemaFilePath = getSchemaPath(schema, directory);
            if (Objects.isNull(schemaFilePath)) {
                LOGGER.error("Invalid schema file path '{}'", subject);
                return null;
            }
            if (dryRun) {
                LOGGER.info("Schema to be downloaded to {}", schemaFilePath);
            } else {
                Files.writeString(schemaFilePath, schemaContent, CREATE, TRUNCATE_EXISTING);
                LOGGER.info("Schema downloaded  to {}", schemaFilePath);
            }

            return schema;
        } catch (RestClientException e) {
            if (e.getErrorCode() == 40401 || e.getErrorCode() == 40403) {
                return null;
            } else {
                throw e;
            }
        }
    }

    public String getContent(final Path schemaFile) throws IOException {
        final String schema = Files.readString(schemaFile);
        if (schema == null || schema.isEmpty() || schema.isBlank()) {
            throw new IOException("Schema '" + schemaFile + "' must not be empty!");
        }
        return schema;
    }

    public Schema updateCompatibility(final Schema schema) throws IOException, RestClientException {
        final String subject = schema.getSubject();
        final Schema.CompatibilityMode compatibilityMode = schema.getCompatibilityMode();
        final String compatibility = getCompatibilityMode(subject);
        if (Objects.isNull(compatibilityMode)) {
            LOGGER.debug("Compatibility for subject '{}' is default '{}'", subject, compatibility);
            return null;
        }
        final String newCompatibility = compatibilityMode.toString();
        if (Objects.equals(compatibility, newCompatibility)) {
            LOGGER.debug("Compatibility for subject '{}' is already '{}'", subject, compatibility);
            return null;
        }
        if (dryRun) {
            LOGGER.info("Compatibility to be updated to '{}' for subject '{}'", newCompatibility, subject);
        } else {
            final String updatedCompatibility = schemaRegistryClient.updateCompatibility(subject, newCompatibility);
            LOGGER.info("Compatibility updated to '{}' for subject '{}'", updatedCompatibility, subject);
        }

        return schema;
    }

    public Schema registerSchema(final Schema schema, final String directory) throws IOException, RestClientException {
        final String subject = schema.getSubject();
        final Schema.Type schemaType = schema.getType();
        final Path schemaFile = getSchemaPath(schema, directory);
        if (Objects.isNull(schemaFile)) {
            LOGGER.error("Invalid schema file '{}'", subject);
            return null;
        }
        final ParsedSchema parsedSchema = parseSchema(schemaType.toString(), schemaFile);
        if (Objects.isNull(parsedSchema)) {
            LOGGER.error("Schema of type {} for subject '{}' is incompatible to existing schemas", schemaType, subject);
            return null;
        }
        /*
        final boolean compatible = schemaRegistryClient.testCompatibility(subject, parsedSchema);
         */
        final boolean compatible = testCompatibility(subject, parsedSchema);
        if (!compatible) {
            LOGGER.error("Schema of type {} for subject '{}' is incompatible to existing schemas", schemaType, subject);
            return null;
        }
        final int version = getVersion(subject, parsedSchema);
        if (version > 0) {
            LOGGER.debug("Schema of type {} for subject and current schema '{}' already exists and has version {}", schemaType, subject, version);
            return null;
        }
        if (dryRun) {
            LOGGER.info("Schema of type {} to be registered for subject '{}'", schemaType, subject);
        } else {
            final int schemaId = schemaRegistryClient.register(subject, parsedSchema);
            LOGGER.info("Schema of type {} registered for subject '{}' with schema ID {}", schemaType, subject, schemaId);
        }

        return schema;
    }

    public int getVersion(final String subject, final ParsedSchema parsedSchema) throws IOException, RestClientException {
        try {
            final List<Integer> allVersions = schemaRegistryClient.getAllVersions(subject);
            if (allVersions.isEmpty()) {
                return 0;
            }
            return schemaRegistryClient.getVersion(subject, parsedSchema);
        } catch (RestClientException e) {
            if (e.getErrorCode() == 40401 || e.getErrorCode() == 40403) {
                return 0;
            } else {
                throw e;
            }
        }
    }

    public ParsedSchema parseSchema(final String type, final Path schemaFile) throws IOException {
        if (schemaFile == null) throw new IllegalArgumentException("Path to schema file must not be null.");

        final String schema = getContent(schemaFile);

        return schemaRegistryClient.parseSchema(type, schema, Collections.emptyList()).orElse(null);
    }

    private void printSchemas(Set<Schema> registeredSchemas) {
        if (Objects.isNull(registeredSchemas) || registeredSchemas.isEmpty()) {
            return;
        }
        System.out.println(getTable(registeredSchemas, Arrays.asList(
                new Column().header("Subject").dataAlign(LEFT).with(Schema::getSubject),
                new Column().header("Type").dataAlign(LEFT).with(schema -> {
                    final Schema.Type type = schema.getType();
                    return Objects.nonNull(type) ? type.toString() : null;
                }),
                new Column().header("Compatibility Mode").dataAlign(LEFT).with(schema -> {
                    final Schema.CompatibilityMode compatibilityMode = schema.getCompatibilityMode();
                    return Objects.nonNull(compatibilityMode) ? compatibilityMode.toString() : "default";
                })

        )));
    }

    public boolean testCompatibility(String subject, ParsedSchema parsedSchema) throws IOException, RestClientException {
        try {
            return schemaRegistryClient.testCompatibility(subject, parsedSchema);
        } catch (RestClientException e) {
            if (e.getErrorCode() == 40401 || e.getErrorCode() == 40403) {
                return true;
            } else {
                throw e;
            }
        }
    }

}
