package de.volkerfaas.kafka.topology.services.impl;

import de.volkerfaas.kafka.topology.ApplicationConfiguration;
import de.volkerfaas.kafka.topology.model.Domain;
import de.volkerfaas.kafka.topology.model.Topic;
import de.volkerfaas.kafka.topology.model.TopologyFile;
import de.volkerfaas.kafka.topology.model.Visibility;
import de.volkerfaas.kafka.topology.repositories.SchemaRegistryRepository;
import org.junit.jupiter.api.*;
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

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class SchemaFileServiceImplTest {

    private SchemaFileServiceImpl schemaFileService;
    private String topologyDirectory;

    @BeforeEach
    void init() {
        Environment environment = mock(Environment.class);
        SchemaRegistryRepository schemaRegistryRepository = mock(SchemaRegistryRepository.class);
        this.schemaFileService = new SchemaFileServiceImpl(environment, schemaRegistryRepository);
        final URL resource = getClass().getClassLoader().getResource("topology-de.volkerfaas.arc.yaml");
        assertNotNull(resource);
        this.topologyDirectory = new File(resource.getPath()).getParent();
        doReturn(topologyDirectory).when(environment).getRequiredProperty(eq(ApplicationConfiguration.PROPERTY_KEY_TOPOLOGY_DIRECTORY));
    }

    @Nested
    class updateSchemaFiles {

        @Test
        void must_register_schema_files_with_full_path() {
            final String keySchemaFile = "events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-key.avsc";
            final String valueSchemaFile = "events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.avsc";
            TopologyFile topology = new TopologyFile();

            Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            topology.setDomain(domain);

            Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            domain.getVisibilities().add(visibility);

            Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setNumPartitions(4);
            topic.setReplicationFactor((short) 2);
            topic.setPrefix("de.volkerfaas.arc.public.");
            topic.setKeySchemaFile(keySchemaFile);
            topic.setValueSchemaFile(valueSchemaFile);
            visibility.getTopics().add(topic);

            final Set<Path> schemaFiles = schemaFileService.updateSchemaFiles(List.of(domain));
            assertNotNull(schemaFiles);
            assertEquals(2, schemaFiles.size());
            assertThat(schemaFiles, containsInAnyOrder(
                    Path.of(topologyDirectory, keySchemaFile),
                    Path.of(topologyDirectory, valueSchemaFile)
            ));
        }

    }

    @Nested
    class addSchemaFiles {

        @Test
        void must_return_a_set_of_path_containing_the_full_paths_to_the_schema_files() {
            Topic topic = new Topic();
            topic.setPrefix("de.volkerfaas.arc.public.");
            topic.setName("user_updated");
            topic.setKeySchemaFile("events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-key.avsc");
            topic.setValueSchemaFile("events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.avsc");

            final Set<Path> schemaFiles = schemaFileService.addSchemaFiles(topic);
            assertEquals(2, schemaFiles.size());
            assertThat(schemaFiles, hasItems(
                    Path.of(topologyDirectory, topic.getKeySchemaFile()),
                    Path.of(topologyDirectory, topic.getValueSchemaFile())
            ));
        }

    }

    @Nested
    class findSchema {

        @Test
        void must_return_null_if_subject_not_in_list_of_subjects() {
            Set<String> subjects = Set.of("de.volkerfaas.arc.public.user_updated-value");
            final String schemaFileName = schemaFileService.findSchema(subjects, "de.volkerfaas.arc", "de.volkerfaas.arc.public.user_updated", "-key");
            assertNull(schemaFileName);
        }

        @Test
        void must_return_a_schema_file_name_if_subject_not_in_list_of_subjects() {
            Set<String> subjects = Set.of("de.volkerfaas.arc.public.user_updated-value");
            final String schemaFileName = schemaFileService.findSchema(subjects, "de.volkerfaas.arc", "de.volkerfaas.arc.public.user_updated", "-value");
            assertNotNull(schemaFileName);
            assertEquals("events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.avsc", schemaFileName);
        }

    }

}
