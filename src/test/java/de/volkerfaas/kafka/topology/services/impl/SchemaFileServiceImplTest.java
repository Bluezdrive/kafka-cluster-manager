package de.volkerfaas.kafka.topology.services.impl;

import de.volkerfaas.kafka.topology.ApplicationConfiguration;
import de.volkerfaas.kafka.topology.model.Domain;
import de.volkerfaas.kafka.topology.model.Topic;
import de.volkerfaas.kafka.topology.model.Visibility;
import de.volkerfaas.kafka.topology.repositories.SchemaRegistryRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.core.env.Environment;

import java.io.File;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@DisplayName("In the class SchemaFileServiceImpl")
class SchemaFileServiceImplTest {

    private SchemaFileServiceImpl schemaFileService;
    private String topologyDirectory;

    @BeforeEach
    void init() {
        final Environment environment = mock(Environment.class);
        final SchemaRegistryRepository schemaRegistryRepository = mock(SchemaRegistryRepository.class);
        this.schemaFileService = new SchemaFileServiceImpl(environment, schemaRegistryRepository);
        final URL resource = getClass().getClassLoader().getResource("topology-de.volkerfaas.arc.yaml");
        assertNotNull(resource);
        this.topologyDirectory = new File(resource.getPath()).getParent();
        doReturn(topologyDirectory).when(environment).getRequiredProperty(eq(ApplicationConfiguration.PROPERTY_KEY_TOPOLOGY_DIRECTORY));
    }

    @Nested
    @DisplayName("the method listSchemaFilesByDomains")
    class ListSchemaFilesByDomains {

        @Test
        @DisplayName("should return all schema files of the domains in the list")
        void testGetAllSchemaFilesOfDomains() {
            final String keySchemaFile = "events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-key.avsc";
            final String valueSchemaFile = "events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.avsc";

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
            topic.setKeySchemaFile(keySchemaFile);
            topic.setValueSchemaFile(valueSchemaFile);
            visibility.getTopics().add(topic);

            final Set<Path> schemaFiles = schemaFileService.listSchemaFilesByDomains(List.of(domain));
            assertNotNull(schemaFiles);
            assertEquals(2, schemaFiles.size());
            assertThat(schemaFiles, containsInAnyOrder(
                    Path.of(topologyDirectory, keySchemaFile),
                    Path.of(topologyDirectory, valueSchemaFile)
            ));
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
            topic.setKeySchemaFile("events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-key.avsc");
            topic.setValueSchemaFile("events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.avsc");

            final Set<Path> schemaFiles = schemaFileService.listSchemaFilesByTopic(topic);
            assertEquals(2, schemaFiles.size());
            assertThat(schemaFiles, hasItems(
                    Path.of(topologyDirectory, topic.getKeySchemaFile()),
                    Path.of(topologyDirectory, topic.getValueSchemaFile())
            ));
        }

    }

    @Nested
    @DisplayName("the method findSchema")
    class FindSchema {

        @Test
        @DisplayName("should return null if subject not in list of subjects")
        void testFindSchemaFail() {
            final Set<String> subjects = Set.of("de.volkerfaas.arc.public.user_updated-value");
            final String schemaFileName = schemaFileService.findSchema(subjects, "de.volkerfaas.arc", "de.volkerfaas.arc.public.user_updated", "-key");
            assertNull(schemaFileName);
        }

        @Test
        @DisplayName("should return a schema file name if subject in list of subjects")
        void testFindSchemaSuccess() {
            final Set<String> subjects = Set.of("de.volkerfaas.arc.public.user_updated-value");
            final String schemaFileName = schemaFileService.findSchema(subjects, "de.volkerfaas.arc", "de.volkerfaas.arc.public.user_updated", "-value");
            assertNotNull(schemaFileName);
            assertEquals("events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.avsc", schemaFileName);
        }

    }

}
