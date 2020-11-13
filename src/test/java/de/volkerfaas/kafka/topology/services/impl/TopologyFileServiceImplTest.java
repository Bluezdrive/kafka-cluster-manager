package de.volkerfaas.kafka.topology.services.impl;

import de.volkerfaas.kafka.topology.model.*;
import de.volkerfaas.kafka.topology.repositories.TopologyFileRepository;
import de.volkerfaas.kafka.topology.services.AccessControlService;
import de.volkerfaas.kafka.topology.services.SchemaFileService;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.springframework.core.env.Environment;

import javax.validation.Validator;
import java.io.File;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class TopologyFileServiceImplTest {

    private SchemaFileService schemaFileService;
    private TopologyFileServiceImpl topologyFileService;
    private TopologyFileRepository topologyFileRepository;
    private TopicServiceImpl topicService;
    private String topologyDirectory;
    private File topologyFile;

    @BeforeEach
    void init() {
        final URL resource = getClass().getClassLoader().getResource("topology-de.volkerfaas.arc.yaml");
        assertNotNull(resource);
        this.topologyFile = new File(resource.getPath());
        this.topologyDirectory = topologyFile.getParent();

        AccessControlService accessControlService = Mockito.mock(AccessControlService.class);
        this.topicService = Mockito.mock(TopicServiceImpl.class);
        this.topologyFileRepository = Mockito.mock(TopologyFileRepository.class);
        Validator validator = Mockito.mock(Validator.class);
        Environment environment = Mockito.mock(Environment.class);
        this.schemaFileService = Mockito.mock(SchemaFileServiceImpl.class);
        this.topologyFileService = new TopologyFileServiceImpl(environment, topicService, accessControlService, schemaFileService, topologyFileRepository, validator);
    }

    @Nested
    class getDomainNameAndVisibilties {

        @Test
        void must_return_a_list_of_pairs_with_domain_name_and_visibility() {
            Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            domain.setPrincipal("User:129849");

            Visibility publicVisibility = new Visibility();
            publicVisibility.setType(Visibility.Type.PUBLIC);
            domain.getVisibilities().add(publicVisibility);

            Visibility privateVisibility = new Visibility();
            privateVisibility.setType(Visibility.Type.PRIVATE);
            domain.getVisibilities().add(privateVisibility);

            final List<Pair<String, Visibility>> pairs = topologyFileService.getDomainNameAndVisibilities(domain);
            assertNotNull(pairs);
            assertEquals(2, pairs.size());
            assertThat(pairs, hasItems(
                    Pair.with(domain.getName(), publicVisibility),
                    Pair.with(domain.getName(), privateVisibility)
            ));
        }

    }

    @Nested
    class getDomainNameAndVisibilityFullNameAndTopics {

        @Test
        void must_return_a_list_of_triplets_containing_domain_name_and_visibility_full_name_and_topic() {
            String domainName = "de.volkerfaas.arc";

            Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");

            Topic topicUserUpdated = new Topic();
            topicUserUpdated.setName("user_updated");
            visibility.getTopics().add(topicUserUpdated);

            Topic topicCardUpdated = new Topic();
            topicCardUpdated.setName("card_updated");
            visibility.getTopics().add(topicCardUpdated);

            Pair<String, Visibility> pair = Pair.with(domainName, visibility);

            final List<Triplet<String, String, Topic>> triplets = topologyFileService.getDomainNameAndVisibilityFullNameAndTopics(pair);
            assertNotNull(triplets);
            assertEquals(2, triplets.size());
            assertThat(triplets, hasItems(
                    Triplet.with(domainName, visibility.getFullName(), topicUserUpdated),
                    Triplet.with(domainName, visibility.getFullName(), topicCardUpdated)
            ));
        }

    }

    @Nested
    class getValueSchemaFilename {

        @Test
        void must_return_a_value_schema_filename() {
            String domainName = "de.volkerfaas.arc";

            Topic topicUserUpdated = new Topic();
            topicUserUpdated.setName("user_updated");
            topicUserUpdated.setPrefix("de.volkerfaas.arc.public.");

            final String valueSchemaFilename = topologyFileService.getValueSchemaFilename(domainName, topicUserUpdated);
            assertEquals("events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.avsc", valueSchemaFilename);
        }

    }

    @Nested
    class addAdditionalValues {

        @Test
        void must_return_a_topology_with_prefix_added_to_visibilities_and_topics() {

            TopologyFile topology = new TopologyFile();
            topology.setFile(topologyFile);

            Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            topology.setDomain(domain);

            Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            domain.getVisibilities().add(visibility);

            Topic topic = new Topic();
            topic.setName("user_updated");
            visibility.getTopics().add(topic);

            TopologyFile topologyWithAdditionalValues = topologyFileService.addAdditionalValues(topology);
            assertNotNull(topologyWithAdditionalValues);
            final Domain domainWithAdditionalValues = topologyWithAdditionalValues.getDomain();
            assertNotNull(domainWithAdditionalValues);
            final Visibility visibilityWithAdditionalValues = domainWithAdditionalValues.getVisibilities().stream().findFirst().orElse(null);
            assertNotNull(visibilityWithAdditionalValues);
            assertEquals(domain.getName() + ".", visibilityWithAdditionalValues.getPrefix());
            final Topic topicWithAdditionalValues = visibilityWithAdditionalValues.getTopics().stream().findFirst().orElse(null);
            assertNotNull(topicWithAdditionalValues);
            assertEquals(visibility.getFullName() + ".", topicWithAdditionalValues.getPrefix());
        }

    }

    @Nested
    class addAdditionalValuesToTopic {

        @Test
        void must_add_the_prefix_and_value_schema_file_name_to_a_topic() {
            String domainName = "de.volkerfaas.arc";
            String visibilityFullName = "de.volkerfaas.arc.public";
            Topic topicUserUpdated = new Topic();
            topicUserUpdated.setName("user_updated");

            Triplet<String, String, Topic> triplet = Triplet.with(domainName, visibilityFullName, topicUserUpdated);
            topologyFileService.addAdditionalValuesToTopic(triplet);
            assertEquals("de.volkerfaas.arc.public.", topicUserUpdated.getPrefix());
            assertEquals("events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.avsc", topicUserUpdated.getValueSchemaFile());
        }

    }

    @Nested
    class addAdditionalValuesToVisibility {

        @Test
        void must_add_the_prefix_to_a_visibility() {
            String domainName = "de.volkerfaas.arc";

            Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);

            Pair<String, Visibility> pair = Pair.with(domainName, visibility);
            final Pair<String, Visibility> result = topologyFileService.addAdditionalValuesToVisibility(pair);
            assertEquals(result, pair);
            assertEquals("de.volkerfaas.arc.", visibility.getPrefix());
        }

    }

    @Nested
    class createTopologyIfNotExists {

        @Test
        void returns_existing_topology_file() {
            final Set<TopologyFile> topologies = new HashSet<>();
            final String pathname = "/topology";
            final String domainName = "de.volkerfaas.arc";

            TopologyFile topology = new TopologyFile();
            topology.setFile(new File(pathname, "topology-" + domainName + ".yaml"));
            topologies.add(topology);

            final TopologyFile existingTopology = topologyFileService.createTopologyIfNotExists(pathname, domainName, topologies);
            assertNotNull(existingTopology);
            assertEquals(topology, existingTopology);
            assertTrue(topologies.contains(existingTopology));
        }

        @Test
        void returns_new_topology_file() {
            final Set<TopologyFile> topologies = new HashSet<>();
            final String pathname = "/topology";
            final String domainName = "de.volkerfaas.arc";

            final TopologyFile newTopology = topologyFileService.createTopologyIfNotExists(pathname, domainName, topologies);
            assertNotNull(newTopology);
            assertEquals("topology-" + domainName + ".yaml", newTopology.getFile().getName());
            assertTrue(topologies.contains(newTopology));
        }

    }

    @Nested
    class createDomainIfNotExists {

        @Test
        void returns_existing_domain() {
            final String pathname = "/topology";
            final String domainName = "de.volkerfaas.arc";

            TopologyFile topology = new TopologyFile();
            topology.setFile(new File(pathname, "topology-" + domainName + ".yaml"));

            Domain domain = new Domain();
            domain.setName(domainName);
            topology.setDomain(domain);

            final Domain existingDomain = topologyFileService.createDomainIfNotExistsInTopology(domainName, topology);
            assertNotNull(existingDomain);
            assertEquals(domain, existingDomain);
            assertEquals(topology.getDomain(), existingDomain);
        }

        @Test
        void returns_new_domain() {
            final String pathname = "/topology";
            final String domainName = "de.volkerfaas.arc";

            TopologyFile topology = new TopologyFile();
            topology.setFile(new File(pathname, "topology-" + domainName + ".yaml"));

            final Domain newDomain = topologyFileService.createDomainIfNotExistsInTopology(domainName, topology);
            assertNotNull(newDomain);
            assertEquals(domainName, newDomain.getName());
            assertEquals(topology.getDomain(), newDomain);
        }

    }

    @Nested
    class createVisibilityIfNotExists {

        @Test
        void returns_existing_visibility() {
            final String domainName = "de.volkerfaas.arc";

            Domain domain = new Domain();
            domain.setName(domainName);

            Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            domain.getVisibilities().add(visibility);

            final Visibility existingVisibility = topologyFileService.createVisibilityIfNotExistsInDomain("public", domain);
            assertNotNull(existingVisibility);
            assertEquals(visibility, existingVisibility);
            assertTrue(domain.getVisibilities().contains(existingVisibility));
        }

        @Test
        void returns_new_visibility() {
            final String domainName = "de.volkerfaas.arc";

            Domain domain = new Domain();
            domain.setName(domainName);

            final Visibility newVisibility = topologyFileService.createVisibilityIfNotExistsInDomain("public", domain);
            assertNotNull(newVisibility);
            assertEquals(Visibility.Type.PUBLIC, newVisibility.getType());
            assertTrue(domain.getVisibilities().contains(newVisibility));
        }

    }

    @Nested
    class restoreTopologies {

        @ParameterizedTest
        @ValueSource(strings = {"-key", "-value"})
        void must_return_a_topology_matching_the_given_configuration(String suffix) throws ExecutionException, InterruptedException {
            final String domainName = "de.volkerfaas.test";
            final String visibilityType = "public";
            final String fullTopicName = domainName + "." + visibilityType + ".user_updated";
            final String subject = fullTopicName + suffix;
            final Set<String> subjects = Set.of(subject);
            Map<String, String> config = new HashMap<>();
            config.put("cleanup.policy", "compact");
            config.put("min.compaction.lag.ms", "100");
            KafkaTopic kafkaTopic = new KafkaTopic(fullTopicName, 4, (short) 1, config);
            doReturn(List.of(kafkaTopic)).when(topicService).listTopicsInCluster();
            doReturn(subjects).when(schemaFileService).listSubjects();
            doCallRealMethod().when(schemaFileService).findSchema(eq(subjects), eq(domainName), eq(fullTopicName), eq(suffix));
            doCallRealMethod().when(topicService).createTopic(anyString(), any(KafkaTopic.class));
            doAnswer(invocation -> invocation.getArgument(0)).when(topologyFileRepository).writeTopology(any(TopologyFile.class));
            doReturn("").when(schemaFileService).downloadSchemaFile(anyString());

            final Set<TopologyFile> topologies = topologyFileService.restoreTopologies(topologyDirectory, List.of(domainName));
            assertNotNull(topologies);
            assertEquals(1, topologies.size());
            final TopologyFile topology = topologies.stream().findFirst().orElse(null);
            assertNotNull(topology);
            assertEquals("topology-" + domainName + ".yaml", topology.getFile().getName());
            final Domain domain = topology.getDomain();
            assertNotNull(domain);
            assertEquals(domainName, domain.getName());
            final Visibility visibility = domain.getVisibilities().get(0);
            assertNotNull(visibility);
            assertEquals(visibilityType, visibility.getType().getValue().toLowerCase());
            final Topic topic = visibility.getTopics().get(0);
            assertNotNull(topic);
            assertEquals(fullTopicName, topic.getFullName());
            assertEquals(4, topic.getNumPartitions());
            assertEquals(1, topic.getReplicationFactor());
            final String cleanupPolicy = topic.getConfig().get("cleanupPolicy");
            assertNotNull(cleanupPolicy);
            assertEquals("compact", cleanupPolicy);
            final String minCompactionLagMs = topic.getConfig().get("minCompactionLagMs");
            assertNotNull(minCompactionLagMs);
            assertEquals("100", minCompactionLagMs);
        }

        @ParameterizedTest
        @ValueSource(strings = {"-key", "-value"})
        void must_return_nothing_in_case_schema_registry_is_empty(String suffix) throws ExecutionException, InterruptedException {
            final String domainName = "de.volkerfaas.test";
            final String visibilityType = "public";
            final String fullTopicName = domainName + "." + visibilityType + ".user_updated";
            final Set<String> subjects = Collections.emptySet();
            Map<String, String> config = new HashMap<>();
            config.put("cleanup.policy", "compact");
            config.put("min.compaction.lag.ms", "100");
            KafkaTopic kafkaTopic = new KafkaTopic(fullTopicName, 4, (short) 1, config);
            doReturn(List.of(kafkaTopic)).when(topicService).listTopicsInCluster();
            doReturn(subjects).when(schemaFileService).listSubjects();
            doCallRealMethod().when(schemaFileService).findSchema(eq(subjects), eq(domainName), eq(fullTopicName), eq(suffix));
            doCallRealMethod().when(topicService).createTopic(anyString(), any(KafkaTopic.class));
            doAnswer(invocation -> invocation.getArgument(0)).when(topologyFileRepository).writeTopology(any(TopologyFile.class));
            doReturn("").when(schemaFileService).downloadSchemaFile(anyString());

            final Set<TopologyFile> topologies = topologyFileService.restoreTopologies(topologyDirectory, List.of(domainName));
            assertNotNull(topologies);
            assertEquals(0, topologies.size());
        }

        @ParameterizedTest
        @ValueSource(strings = {"-key", "-value"})
        void must_return_nothing_in_case_schema_registry_is_null(String suffix) throws ExecutionException, InterruptedException {
            final String domainName = "de.volkerfaas.test";
            final String visibilityType = "public";
            final String fullTopicName = domainName + "." + visibilityType + ".user_updated";
            Map<String, String> config = new HashMap<>();
            config.put("cleanup.policy", "compact");
            config.put("min.compaction.lag.ms", "100");
            KafkaTopic kafkaTopic = new KafkaTopic(fullTopicName, 4, (short) 1, config);
            doReturn(List.of(kafkaTopic)).when(topicService).listTopicsInCluster();
            doReturn(null).when(schemaFileService).listSubjects();
            doCallRealMethod().when(schemaFileService).findSchema(isNull(), eq(domainName), eq(fullTopicName), eq(suffix));
            doCallRealMethod().when(topicService).createTopic(anyString(), any(KafkaTopic.class));
            doAnswer(invocation -> invocation.getArgument(0)).when(topologyFileRepository).writeTopology(any(TopologyFile.class));
            doReturn("").when(schemaFileService).downloadSchemaFile(anyString());

            final Set<TopologyFile> topologies = topologyFileService.restoreTopologies(topologyDirectory, List.of(domainName));
            assertNotNull(topologies);
            assertEquals(0, topologies.size());
        }

        @ParameterizedTest
        @ValueSource(strings = {"-key", "-value"})
        void must_return_nothing_in_case_kafka_cluster_not_available(String suffix) throws ExecutionException, InterruptedException {
            final String domainName = "de.volkerfaas.test";
            final String visibilityType = "public";
            final String fullTopicName = domainName + "." + visibilityType + ".user_updated";
            final String subject = fullTopicName + suffix;
            final Set<String> subjects = Set.of(subject);
            doReturn(null).when(topicService).listTopicsInCluster();
            doReturn(subjects).when(schemaFileService).listSubjects();
            doCallRealMethod().when(schemaFileService).findSchema(eq(subjects), eq(domainName), eq(fullTopicName), eq(suffix));
            doCallRealMethod().when(topicService).createTopic(anyString(), any(KafkaTopic.class));
            doAnswer(invocation -> invocation.getArgument(0)).when(topologyFileRepository).writeTopology(any(TopologyFile.class));
            doReturn("").when(schemaFileService).downloadSchemaFile(anyString());

            final Set<TopologyFile> topologies = topologyFileService.restoreTopologies(topologyDirectory, List.of(domainName));
            assertNotNull(topologies);
            assertEquals(0, topologies.size());
        }

    }

    @Nested
    class listDomains {

        @Test
        void must_return_the_domains_mentioned_in_all_topology_files() {
            TopologyFile topologyFileArc = new TopologyFile();
            Domain domainArc = new Domain();
            domainArc.setName("de.volkerfaas.arc");
            topologyFileArc.setDomain(domainArc);

            TopologyFile topologyFileFoo = new TopologyFile();
            Domain domainFoo = new Domain();
            domainFoo.setName("de.volkerfaas.foo");
            topologyFileFoo.setDomain(domainFoo);

            TopologyFile topologyFileBar = new TopologyFile();
            Domain domainBar = new Domain();
            domainBar.setName("de.volkerfaas.bar");
            topologyFileBar.setDomain(domainBar);

            final Set<TopologyFile> topologyFiles = Set.of(topologyFileArc, topologyFileFoo, topologyFileBar);
            final List<Domain> domains = topologyFileService.listDomains(topologyFiles);
            assertEquals(3, domains.size());
            assertThat(domains, hasItems(
                    domainArc,
                    domainFoo,
                    domainBar
            ));
        }

    }

}
