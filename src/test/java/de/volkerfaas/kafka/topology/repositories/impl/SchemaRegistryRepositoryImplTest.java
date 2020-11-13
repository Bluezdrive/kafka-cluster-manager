package de.volkerfaas.kafka.topology.repositories.impl;

import de.volkerfaas.kafka.topology.ApplicationConfiguration;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.junit.jupiter.api.*;
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

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
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
    class getSchemaSubject {

        @Test
        void must_return_the_schema_subject_derived_from_the_filename() {
            String directory = "/home/travis/kafka-topology/topology";
            String valueSchemaFile = "events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.avsc";
            Path schemaFile = Path.of(directory, valueSchemaFile);
            String subject = schemaRegistryRepository.getSchemaSubject(schemaFile);
            assertEquals("de.volkerfaas.arc.public.user_updated-value", subject);
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "events/user_updated-value.avsc",
                "events/de.volkerfaas.arc/user_updated-value.avsc"
        })
        void must_throw_an_exception_in_case_of_an_invalid_filename(String valueSchemaFile) {
            String directory = "/home/travis/kafka-topology/topology";
            Path schemaFile = Path.of(directory, valueSchemaFile);
            Exception exception = assertThrows(IllegalArgumentException.class, () -> schemaRegistryRepository.getSchemaSubject(schemaFile));
            assertEquals("Schema filename is not valid: " + schemaFile.toString(), exception.getMessage());
        }

    }

    @Nested
    class getContent {

        @ParameterizedTest
        @NullAndEmptySource
        @ValueSource(strings = {" "})
        void must_throw_an_exception_in_case_the_file_contains_nothing(String content) {
            try (MockedStatic<Files> filesMock = mockStatic(Files.class)) {
                filesMock.when(() -> Files.readString(any(Path.class))).thenReturn(content);
                final Path path = Path.of(schemaFilePath);
                Exception exception = assertThrows(IllegalStateException.class, () -> schemaRegistryRepository.getContent(path));
                assertEquals("Schema '" + schemaFilePath + "' must not be empty!", exception.getMessage());
            }
        }

    }

    @Nested
    class downloadSchemaFile {

        @Test
        void must_download_schema_file_and_return_schema_file_path() throws IOException, RestClientException {
            final String schema = "{ \"type\": \"string\" }";
            final String subject = "de.volkerfaas.test.public.user_updated-value";
            final String schemaFile = "events/de.volkerfaas.test/" + subject + ".avsc";
            SchemaMetadata schemaMetadata = new SchemaMetadata(10000, 1, schema);
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
        void must_return_null(String schemaFile) {
            final String schemaFilePath = schemaRegistryRepository.downloadSchemaFile(schemaFile);
            assertNull(schemaFilePath);
        }

    }

}