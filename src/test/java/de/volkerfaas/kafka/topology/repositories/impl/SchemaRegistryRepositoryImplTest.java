package de.volkerfaas.kafka.topology.repositories.impl;

import de.volkerfaas.kafka.topology.model.Schema;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@DisplayName("In the class SchemaRegistryRepositoryImpl")
class SchemaRegistryRepositoryImplTest {

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
        this.schemaRegistryClient = mock(CachedSchemaRegistryClient.class);
        this.schemaRegistryRepository = new SchemaRegistryRepositoryImpl(schemaRegistryClient, null);
        final URL schemaResource = this.getClass()
                .getClassLoader()
                .getResource("events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.avsc");
        assert schemaResource != null;
        this.schemaFilePath = schemaResource.getPath();
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
                final Exception exception = assertThrows(IOException.class, () -> schemaRegistryRepository.getContent(path));
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
            final String schemaContent = "{ \"type\": \"string\" }";
            final String subject = "de.volkerfaas.test.public.user_updated-value";
            final String schemaFile = "events/de.volkerfaas.test/" + subject + ".avsc";
            final SchemaMetadata schemaMetadata = new SchemaMetadata(10000, 1, "AVRO", Collections.emptyList(), schemaContent);
            doReturn(schemaMetadata).when(schemaRegistryClient).getLatestSchemaMetadata(eq(subject));
            try (MockedStatic<Files> filesMock = mockStatic(Files.class)) {
                filesMock.when(() -> Files.createDirectories(any(Path.class))).thenAnswer((Answer<Path>) invocation -> (Path) invocation.getArguments()[0]);
                filesMock.when(() -> Files.writeString(any(Path.class), eq(schemaContent), eq(StandardOpenOption.CREATE), eq(StandardOpenOption.TRUNCATE_EXISTING))).thenAnswer((Answer<Path>) invocation -> {
                    final Path pathToWrite = (Path) invocation.getArguments()[0];
                    assertNotNull(pathToWrite);
                    assertEquals(topologyDirectory + "/" + schemaFile, pathToWrite.toString());

                    final String schemaToWrite = (String) invocation.getArguments()[1];
                    assertNotNull(schemaToWrite);
                    assertEquals(schemaContent, schemaToWrite);

                    return pathToWrite;
                });
                final Schema schema = new Schema(subject, null, null);
                final Schema downloadedSchema = schemaRegistryRepository.downloadSchema(schema, topologyDirectory);
                assertNotNull(downloadedSchema);
                assertEquals(schema, downloadedSchema);
            }
        }

        @ParameterizedTest
        @NullAndEmptySource
        @ValueSource(strings = { " ", "foo" })
        @DisplayName("should not download schema file and return null when schema subject is invalid")
        void testNotDownloadSchemaFile(String subject) throws IOException, RestClientException {
            final Schema schema = new Schema(subject, Schema.Type.AVRO, Schema.CompatibilityMode.FORWARD_TRANSITIVE);
            final Schema downloadedSchema = schemaRegistryRepository.downloadSchema(schema, topologyDirectory);
            assertNull(downloadedSchema);
        }

    }

    @Nested
    @DisplayName("the method updateCompatibility")
    class UpdateCompatibility {

        @Test
        @DisplayName("should not update compatibility when local equals registry")
        void testUpdateCompatibilityEquals() throws IOException, RestClientException {
            final String subject = "de.volkerfaas.test.public.user_updated-value";
            final String compatibility = "FORWARD_TRANSITIVE";
            final String schemaFile = "events/de.volkerfaas.test/" + subject + ".avsc";
            final Schema schema = new Schema(subject, Schema.Type.AVRO, Schema.CompatibilityMode.valueOf(compatibility));
            doReturn(compatibility).when(schemaRegistryClient).getCompatibility(eq(subject));
            schemaRegistryRepository.updateCompatibility(schema);
            verify(schemaRegistryClient, never()).updateCompatibility(anyString(), anyString());
        }

        @Test
        @DisplayName("should not create compatibility when local equals registry")
        void testUpdateCompatibilityNewEquals() throws IOException, RestClientException {
            final String subject = "de.volkerfaas.test.public.user_updated-value";
            final String compatibility = "FORWARD_TRANSITIVE";
            final String schemaFile = "events/de.volkerfaas.test/" + subject + ".avsc";
            final Schema schema = new Schema(subject, Schema.Type.AVRO, Schema.CompatibilityMode.valueOf(compatibility));
            doThrow(new RestClientException("Not found", 0, 40401)).when(schemaRegistryClient).getCompatibility(eq(subject));
            doReturn("FORWARD_TRANSITIVE").when(schemaRegistryClient).getCompatibility(isNull());
            schemaRegistryRepository.updateCompatibility(schema);
            verify(schemaRegistryClient, never()).updateCompatibility(anyString(), anyString());
        }

        @Test
        @DisplayName("should update compatibility when local not equal registry")
        void testUpdateCompatibilityNewNotEquals() throws IOException, RestClientException {
            final String subject = "de.volkerfaas.test.public.user_updated-value";
            final String compatibility = "FULL_TRANSITIVE";
            final String schemaFile = "events/de.volkerfaas.test/" + subject + ".avsc";
            final Schema schema = new Schema(subject, Schema.Type.AVRO, Schema.CompatibilityMode.valueOf(compatibility));
            doThrow(new RestClientException("Not found", 0, 40401)).when(schemaRegistryClient).getCompatibility(eq(subject));
            doReturn("FORWARD_TRANSITIVE").when(schemaRegistryClient).getCompatibility(isNull());
            doAnswer(invocation -> {
                final String requestedSubject = invocation.getArgument(0);
                assertNotNull(requestedSubject);
                assertEquals(subject, requestedSubject);
                final String requestedCompatibility = invocation.getArgument(1);
                assertNotNull(requestedCompatibility);
                assertEquals(compatibility, requestedCompatibility);

                return null;
            }).when(schemaRegistryClient).updateCompatibility(eq(subject), eq(compatibility));
            schemaRegistryRepository.updateCompatibility(schema);
        }



        @Test
        @DisplayName("should not update compatibility when local is not set")
        void testUpdateCompatibilityNewNull() throws IOException, RestClientException {
            final String subject = "de.volkerfaas.test.public.user_updated-value";
            final String schemaFile = "events/de.volkerfaas.test/" + subject + ".avsc";
            final Schema schema = new Schema(subject, Schema.Type.AVRO, Schema.CompatibilityMode.FORWARD_TRANSITIVE);
            doThrow(new RestClientException("Not found", 0, 40401)).when(schemaRegistryClient).getCompatibility(eq(subject));
            doReturn("FORWARD_TRANSITIVE").when(schemaRegistryClient).getCompatibility(isNull());
            schemaRegistryRepository.updateCompatibility(schema);
            verify(schemaRegistryClient, never()).updateCompatibility(anyString(), anyString());
        }

    }

    @Nested
    @DisplayName("the method registerSchema")
    class RegisterSchema {

        @Test
        @DisplayName("should not register schema file when new schema is not compatible")
        void testSchemaIncompatible() throws IOException, RestClientException {
            final String subject = "de.volkerfaas.test.public.test_created-value";
            final String compatibility = "FULL_TRANSITIVE";
            final String schemaFile = "events/de.volkerfaas.test/" + subject + ".avsc";
            final Schema schema = new Schema(subject, Schema.Type.AVRO, Schema.CompatibilityMode.valueOf(compatibility));
            final String schemaString = "{ \"type\": \"string\" }";
            doReturn(Optional.of(new AvroSchema(schemaString))).when(schemaRegistryClient).parseSchema(eq(AvroSchema.TYPE), anyString(), anyList());
            doReturn(false).when(schemaRegistryClient).testCompatibility(eq(subject), any(ParsedSchema.class));
            schemaRegistryRepository.registerSchema(schema, topologyDirectory);
            verify(schemaRegistryClient, never()).getVersion(anyString(), any(ParsedSchema.class));
            verify(schemaRegistryClient, never()).register(anyString(), any(ParsedSchema.class));
        }

        @Test
        @DisplayName("should not register schema file when schema already exists")
        void testSchemaAlreadyExists() throws IOException, RestClientException {
            final String subject = "de.volkerfaas.test.public.test_created-value";
            final String compatibility = "FULL_TRANSITIVE";
            final String schemaFile = "events/de.volkerfaas.test/" + subject + ".avsc";
            final Schema schema = new Schema(subject, Schema.Type.AVRO, Schema.CompatibilityMode.valueOf(compatibility));
            final String schemaString = "{ \"type\": \"string\" }";
            doReturn(Optional.of(new AvroSchema(schemaString))).when(schemaRegistryClient).parseSchema(eq(AvroSchema.TYPE), anyString(), anyList());
            doReturn(true).when(schemaRegistryClient).testCompatibility(eq(subject), any(ParsedSchema.class));
            doReturn(List.of(1, 2, 3)).when(schemaRegistryClient).getAllVersions(eq(subject));
            doReturn(3).when(schemaRegistryClient).getVersion(eq(subject), any(ParsedSchema.class));
            schemaRegistryRepository.registerSchema(schema, topologyDirectory);
            verify(schemaRegistryClient, never()).register(anyString(), any(ParsedSchema.class));
        }

        @Test
        @DisplayName("should register a new schema when version not exists")
        void testSchemaNewVersion() throws IOException, RestClientException {
            final String subject = "de.volkerfaas.test.public.test_created-value";
            final String compatibility = "FULL_TRANSITIVE";
            final String schemaFile = "events/de.volkerfaas.test/" + subject + ".avsc";
            final Schema schema = new Schema(subject, Schema.Type.AVRO, Schema.CompatibilityMode.valueOf(compatibility));
            final String schemaString = "{ \"type\": \"string\" }";
            doReturn(Optional.of(new AvroSchema(schemaString))).when(schemaRegistryClient).parseSchema(eq(AvroSchema.TYPE), anyString(), anyList());
            doReturn(true).when(schemaRegistryClient).testCompatibility(eq(subject), any(ParsedSchema.class));
            doReturn(Collections.emptyList()).when(schemaRegistryClient).getAllVersions(eq(subject));
            doAnswer(invocation -> {
                final String requestedSubject = invocation.getArgument(0);
                assertNotNull(requestedSubject);
                assertEquals(subject, requestedSubject);
                final ParsedSchema requestedSchema = invocation.getArgument(1);
                assertNotNull(requestedSchema);
                assertEquals("\"string\"", requestedSchema.toString());

                return 1;
            }).when(schemaRegistryClient).register(eq(subject), any(ParsedSchema.class));
            schemaRegistryRepository.registerSchema(schema, topologyDirectory);
            verify(schemaRegistryClient, never()).getVersion(anyString(), any(ParsedSchema.class));
        }



        @Test
        @DisplayName("should register a new schema when subject not exists")
        void testSchemaNewSubject() throws IOException, RestClientException {
            final String subject = "de.volkerfaas.test.public.test_created-value";
            final String compatibility = "FULL_TRANSITIVE";
            final String schemaFile = "events/de.volkerfaas.test/" + subject + ".avsc";
            final Schema schema = new Schema(subject, Schema.Type.AVRO, Schema.CompatibilityMode.valueOf(compatibility));
            final String schemaString = "{ \"type\": \"string\" }";
            doReturn(Optional.of(new AvroSchema(schemaString))).when(schemaRegistryClient).parseSchema(eq(AvroSchema.TYPE), anyString(), anyList());
            doReturn(true).when(schemaRegistryClient).testCompatibility(eq(subject), any(ParsedSchema.class));
            doThrow(new RestClientException("Not found", 0, 40401)).when(schemaRegistryClient).getAllVersions(eq(subject));
            doAnswer(invocation -> {
                final String requestedSubject = invocation.getArgument(0);
                assertNotNull(requestedSubject);
                assertEquals(subject, requestedSubject);
                final ParsedSchema requestedSchema = invocation.getArgument(1);
                assertNotNull(requestedSchema);
                assertEquals("\"string\"", requestedSchema.toString());

                return 1;
            }).when(schemaRegistryClient).register(eq(subject), any(ParsedSchema.class));
            schemaRegistryRepository.registerSchema(schema, topologyDirectory);
            verify(schemaRegistryClient, never()).getVersion(anyString(), any(ParsedSchema.class));
        }

        @Test
        @DisplayName("should register a new schema when subject exists, but schema not exists")
        void testSchemaUpdate() throws IOException, RestClientException {
            final String subject = "de.volkerfaas.test.public.test_created-value";
            final String compatibility = "FULL_TRANSITIVE";
            final String schemaFile = "events/de.volkerfaas.test/" + subject + ".avsc";
            final Schema schema = new Schema(subject, Schema.Type.AVRO, Schema.CompatibilityMode.valueOf(compatibility));
            final String schemaString = "{ \"type\": \"string\" }";
            doReturn(Optional.of(new AvroSchema(schemaString))).when(schemaRegistryClient).parseSchema(eq(AvroSchema.TYPE), anyString(), anyList());
            doReturn(true).when(schemaRegistryClient).testCompatibility(eq(subject), any(ParsedSchema.class));
            doReturn(List.of(1, 2, 3)).when(schemaRegistryClient).getAllVersions(eq(subject));
            doReturn(0).when(schemaRegistryClient).getVersion(eq(subject), any(ParsedSchema.class));
            doAnswer(invocation -> {
                final String requestedSubject = invocation.getArgument(0);
                assertNotNull(requestedSubject);
                assertEquals(subject, requestedSubject);
                final ParsedSchema requestedSchema = invocation.getArgument(1);
                assertNotNull(requestedSchema);
                assertEquals("\"string\"", requestedSchema.toString());

                return 4;
            }).when(schemaRegistryClient).register(eq(subject), any(ParsedSchema.class));
            schemaRegistryRepository.registerSchema(schema, topologyDirectory);
        }




    }

    @Nested
    @DisplayName("the method registerSchema")
    class RegisterSchemaDryRun {

        @BeforeEach
        void init() {
            schemaRegistryRepository = new SchemaRegistryRepositoryImpl(schemaRegistryClient, "");
        }

        @Test
        @DisplayName("should not register a new schema when dry run is active")
        void testSchemaUpdateDryRun() throws IOException, RestClientException {
            final String subject = "de.volkerfaas.test.public.test_created-value";
            final String compatibility = "FULL_TRANSITIVE";
            final String schemaFile = "events/de.volkerfaas.test/" + subject + ".avsc";
            final Schema schema = new Schema(subject, Schema.Type.AVRO, Schema.CompatibilityMode.valueOf(compatibility));
            final String schemaString = "{ \"type\": \"string\" }";
            doReturn(Optional.of(new AvroSchema(schemaString))).when(schemaRegistryClient).parseSchema(eq(AvroSchema.TYPE), anyString(), anyList());
            doReturn(true).when(schemaRegistryClient).testCompatibility(eq(subject), any(ParsedSchema.class));
            doReturn(List.of(1, 2, 3)).when(schemaRegistryClient).getAllVersions(eq(subject));
            doReturn(0).when(schemaRegistryClient).getVersion(eq(subject), any(ParsedSchema.class));
            schemaRegistryRepository.registerSchema(schema, topologyDirectory);
            verify(schemaRegistryClient, never()).register(anyString(), any(ParsedSchema.class));
        }
    }

}