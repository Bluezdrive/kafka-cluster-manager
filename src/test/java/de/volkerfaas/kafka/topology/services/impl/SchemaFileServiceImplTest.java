package de.volkerfaas.kafka.topology.services.impl;

import de.volkerfaas.kafka.topology.model.Domain;
import de.volkerfaas.kafka.topology.model.Schema;
import de.volkerfaas.kafka.topology.model.Topic;
import de.volkerfaas.kafka.topology.model.Visibility;
import de.volkerfaas.kafka.topology.repositories.SchemaRegistryRepository;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@DisplayName("In the class SchemaFileServiceImpl")
class SchemaFileServiceImplTest {

    private SchemaFileServiceImpl schemaFileService;
    private SchemaRegistryRepository schemaRegistryRepository;

    @BeforeEach
    void init() {
        this.schemaRegistryRepository = mock(SchemaRegistryRepository.class);
        this.schemaFileService = new SchemaFileServiceImpl(schemaRegistryRepository);
    }

    @Nested
    @DisplayName("the method listSchemaFilesByDomains")
    class ListSchemaFilesByDomains {

        @Test
        @DisplayName("should return all schema files of the domains in the list")
        void testGetAllSchemaFilesOfDomains() {
            final String keySchemaSubject = "de.volkerfaas.arc.public.user_updated-key";
            final String valueSchemaSubject = "de.volkerfaas.arc.public.user_updated-value";

            final Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");

            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            domain.getVisibilities().add(visibility);

            final Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setNumPartitions(4);
            topic.setReplicationFactor((short) 2);
            topic.setPrefix("de.volkerfaas.arc.public.");
            final Schema keySchema = new Schema(keySchemaSubject, Schema.Type.AVRO, Schema.CompatibilityMode.FORWARD_TRANSITIVE);
            topic.setKeySchema(keySchema);
            final Schema valueSchema = new Schema(valueSchemaSubject, Schema.Type.AVRO, Schema.CompatibilityMode.FORWARD_TRANSITIVE);
            topic.setValueSchema(valueSchema);
            visibility.getTopics().add(topic);

            final Set<Schema> schemas = schemaFileService.listSchemasByDomains(List.of(domain));
            assertNotNull(schemas);
            assertEquals(2, schemas.size());
            assertThat(schemas, containsInAnyOrder(keySchema, valueSchema));
        }

    }

    @Nested
    @DisplayName("the method listSchemaFilesByTopic")
    class ListSchemaFilesByTopic {

        @Test
        @DisplayName("should return the full path of all schema files associated with the given topic")
        void testGetAllSchemaFilesOfTopic() {
            final Topic topic = new Topic();
            topic.setPrefix("de.volkerfaas.arc.public.");
            topic.setName("user_updated");
            final Schema keySchema = new Schema("de.volkerfaas.arc.public.user_updated-key", Schema.Type.AVRO, Schema.CompatibilityMode.FORWARD_TRANSITIVE);
            topic.setKeySchema(keySchema);
            final Schema valueSchema = new Schema("de.volkerfaas.arc.public.user_updated-value", Schema.Type.AVRO, Schema.CompatibilityMode.FORWARD_TRANSITIVE);
            topic.setValueSchema(valueSchema);

            final Set<Schema> schemas = schemaFileService.listSchemaFilesByTopic(topic);
            assertEquals(2, schemas.size());
            assertThat(schemas, hasItems(keySchema, valueSchema));
        }

    }

    @Nested
    @DisplayName("the method findSchema")
    class FindSchema {

        @Test
        @DisplayName("should return null if subject not in list of subjects")
        void testFindSchemaFail() throws IOException, RestClientException {
            doReturn("FORWARD_TRANSITIVE").when(schemaRegistryRepository).getCompatibilityMode(anyString());
            final Set<String> subjects = Set.of("de.volkerfaas.arc.public.user_updated-value");
            final Schema schema = schemaFileService.findSchema(subjects, "de.volkerfaas.arc", "de.volkerfaas.arc.public.user_updated", "-key");
            assertNull(schema);
        }

        @Test
        @DisplayName("should return a schema file name if subject in list of subjects")
        void testFindSchemaSuccess() throws IOException, RestClientException {
            doReturn("FORWARD_TRANSITIVE").when(schemaRegistryRepository).getCompatibilityMode(anyString());
            doReturn("AVRO").when(schemaRegistryRepository).getSchemaType(anyString());
            final Set<String> subjects = Set.of("de.volkerfaas.arc.public.user_updated-value");
            final Schema schema = schemaFileService.findSchema(subjects, "de.volkerfaas.arc", "de.volkerfaas.arc.public.user_updated", "-value");
            assertNotNull(schema);
            assertEquals("de.volkerfaas.arc.public.user_updated-value", schema.getSubject());
            assertEquals(Schema.Type.AVRO, schema.getType());
        }

    }

}
