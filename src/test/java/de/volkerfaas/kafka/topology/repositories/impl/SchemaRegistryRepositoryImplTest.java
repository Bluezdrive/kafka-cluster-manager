package de.volkerfaas.kafka.topology.repositories.impl;

import de.volkerfaas.kafka.topology.ApplicationConfiguration;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.stubbing.Answer;
import org.springframework.core.env.Environment;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@DisplayName("In the class SchemaRegistryRepositoryImpl")
class SchemaRegistryRepositoryImplTest {

    private Environment environment;
    private SchemaRegistryRepositoryImpl schemaRegistryRepository;
    private String schemaFilePath;
    private String topologyDirectory;
    private SchemaRegistryClient schemaRegistryClient;


    @BeforeEach
    void init() {
        final URL topologyResource = getClass().getClassLoader().getResource("topology-de.volkerfaas.arc.yaml");
        assertNotNull(topologyResource);
        final File topologyFile = new File(topologyResource.getPath());
        this.topologyDirectory = topologyFile.getParent();
        this.environment = mock(Environment.class);
        this.schemaRegistryClient = mock(SchemaRegistryClient.class);
        this.schemaRegistryRepository = new SchemaRegistryRepositoryImpl(environment, schemaRegistryClient);
        final URL schemaResource = this.getClass()
                .getClassLoader()
                .getResource("events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.avsc");
        assert schemaResource != null;
        this.schemaFilePath = schemaResource.getPath();
    }

    @Nested
    @DisplayName("the method getSchemaSubject")
    class GetSchemaSubject {

        @Test
        @DisplayName("should return the schema subject derived from the filename")
        void testGetSchemaSubject() {
            final String directory = "/home/travis/kafka-topology/topology";
            final String valueSchemaFile = "events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.avsc";
            final Path schemaFile = Path.of(directory, valueSchemaFile);
            final String subject = schemaRegistryRepository.getSchemaSubject(schemaFile);
            assertEquals("de.volkerfaas.arc.public.user_updated-value", subject);
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "events/user_updated-value.avsc",
                "events/de.volkerfaas.arc/user_updated-value.avsc"
        })
        @DisplayName("should throw an exception in case of an invalid filename")
        void testGetSchemaSubjectException(String valueSchemaFile) {
            final String directory = "/home/travis/kafka-topology/topology";
            final Path schemaFile = Path.of(directory, valueSchemaFile);
            final Exception exception = assertThrows(IllegalArgumentException.class, () -> schemaRegistryRepository.getSchemaSubject(schemaFile));
            assertEquals("Schema filename is not valid: " + schemaFile.toString(), exception.getMessage());
        }

    }

    @Nested
    @DisplayName("the method getContent")
    class GetContent {

        @ParameterizedTest
        @NullAndEmptySource
        @ValueSource(strings = {" "})
        @DisplayName("should throw an exception in case the file contains nothing")
        void testGetContentException(String content) {
            try (MockedStatic<Files> filesMock = mockStatic(Files.class)) {
                filesMock.when(() -> Files.readString(any(Path.class))).thenReturn(content);
                final Path path = Path.of(schemaFilePath);
                final Exception exception = assertThrows(IllegalStateException.class, () -> schemaRegistryRepository.getContent(path));
                assertEquals("Schema '" + schemaFilePath + "' must not be empty!", exception.getMessage());
            }
        }

    }

    @Nested
    @DisplayName("the method downloadSchemaFile")
    class DownloadSchemaFile {

        @Test
        @DisplayName("should download schema file and return schema file path")
        void testDownloadSchemaFile() throws IOException, RestClientException {
            final String schema = "{ \"type\": \"string\" }";
            final String subject = "de.volkerfaas.test.public.user_updated-value";
            final String schemaFile = "events/de.volkerfaas.test/" + subject + ".avsc";
            final SchemaMetadata schemaMetadata = new SchemaMetadata(10000, 1, schema);
            doReturn(topologyDirectory).when(environment).getRequiredProperty(ApplicationConfiguration.PROPERTY_KEY_TOPOLOGY_DIRECTORY);
            doReturn(schemaMetadata).when(schemaRegistryClient).getLatestSchemaMetadata(eq(subject));
            try (MockedStatic<Files> filesMock = mockStatic(Files.class)) {
                filesMock.when(() -> Files.createDirectories(any(Path.class))).thenAnswer((Answer<Path>) invocation -> (Path) invocation.getArguments()[0]);
                filesMock.when(() -> Files.writeString(any(Path.class), eq(schema), eq(StandardOpenOption.CREATE), eq(StandardOpenOption.TRUNCATE_EXISTING))).thenAnswer((Answer<Path>) invocation -> {
                    final Path pathToWrite = (Path) invocation.getArguments()[0];
                    assertNotNull(pathToWrite);
                    assertEquals(topologyDirectory + "/" + schemaFile, pathToWrite.toString());

                    final String schemaToWrite = (String) invocation.getArguments()[1];
                    assertNotNull(schemaToWrite);
                    assertEquals(schema, schemaToWrite);

                    return pathToWrite;
                });
                final String schemaFilePath = schemaRegistryRepository.downloadSchemaFile(schemaFile);
                assertNotNull(schemaFilePath);
                assertEquals(topologyDirectory + "/" + schemaFile, schemaFilePath);

            }
        }

        @ParameterizedTest
        @NullAndEmptySource
        @ValueSource(strings = { " ", "events/test/test.avsc" })
        @DisplayName("should not download schema file and return null when schema file is invalid")
        void testNotDownloadSchemaFile(String schemaFile) {
            final String schemaFilePath = schemaRegistryRepository.downloadSchemaFile(schemaFile);
            assertNull(schemaFilePath);
        }

    }

}