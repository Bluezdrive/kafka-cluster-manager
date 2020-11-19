package de.volkerfaas.kafka.topology.services.impl;

import de.volkerfaas.kafka.cluster.model.PartitionConfiguration;
import de.volkerfaas.kafka.cluster.model.TopicConfiguration;
import de.volkerfaas.kafka.topology.model.*;
import de.volkerfaas.kafka.topology.repositories.TopologyFileRepository;
import de.volkerfaas.kafka.topology.services.AccessControlService;
import de.volkerfaas.kafka.topology.services.SchemaFileService;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.validation.Validator;
import java.io.File;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("In the class TopologyFileServiceImpl")
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

        final AccessControlService accessControlService = mock(AccessControlService.class);
        this.topicService = mock(TopicServiceImpl.class);
        this.topologyFileRepository = mock(TopologyFileRepository.class);
        final Validator validator = mock(Validator.class);
        this.schemaFileService = mock(SchemaFileServiceImpl.class);
        this.topologyFileService = new TopologyFileServiceImpl(topicService, accessControlService, schemaFileService, topologyFileRepository, validator);
    }

    @Nested
    @DisplayName("the method getDomainNameAndVisibilties")
    class GetDomainNameAndVisibilties {

        @Test
        @DisplayName("should return a list of pairs with domain name and visibility")
        void testGetDomainNameAndVisibilties() {
            final Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            domain.setPrincipal("User:129849");

            final Visibility publicVisibility = new Visibility();
            publicVisibility.setType(Visibility.Type.PUBLIC);
            domain.getVisibilities().add(publicVisibility);

            final Visibility privateVisibility = new Visibility();
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
    @DisplayName("the method getDomainNameAndVisibilityFullNameAndTopics")
    class GetDomainNameAndVisibilityFullNameAndTopics {

        @Test
        @DisplayName("should return a list of triplets containing domain name and visibility full name and topic")
        void testGetDomainNameAndVisibilityFullNameAndTopics() {
            final String domainName = "de.volkerfaas.arc";

            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");

            final Topic topicUserUpdated = new Topic();
            topicUserUpdated.setName("user_updated");
            visibility.getTopics().add(topicUserUpdated);

            final Topic topicCardUpdated = new Topic();
            topicCardUpdated.setName("card_updated");
            visibility.getTopics().add(topicCardUpdated);

            final Pair<String, Visibility> pair = Pair.with(domainName, visibility);

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
    @DisplayName("the method getValueSchemaFilename")
    class GetValueSchemaFilename {

        @Test
        @DisplayName("should return a value schema filename")
        void testGetValueSchemaFilename() {
            final String domainName = "de.volkerfaas.arc";

            final Topic topicUserUpdated = new Topic();
            topicUserUpdated.setName("user_updated");
            topicUserUpdated.setPrefix("de.volkerfaas.arc.public.");

            final String valueSchemaFilename = topologyFileService.getValueSchemaFilename(domainName, topicUserUpdated);
            assertEquals("events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.avsc", valueSchemaFilename);
        }

    }

    @Nested
    @DisplayName("the method addAdditionalValues")
    class AddAdditionalValues {

        @Test
        @DisplayName("should return a topology with prefix added to visibilities and topics")
        void testAddAdditionalValues() {
            final TopologyFile topology = new TopologyFile();
            topology.setFile(topologyFile);

            final Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            topology.setDomain(domain);

            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            domain.getVisibilities().add(visibility);

            final Topic topic = new Topic();
            topic.setName("user_updated");
            visibility.getTopics().add(topic);

            final TopologyFile topologyWithAdditionalValues = topologyFileService.addAdditionalValues(topology);
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
    @DisplayName("the method addAdditionalValuesToTopic")
    class AddAdditionalValuesToTopic {

        @Test
        @DisplayName("should add the prefix and value schema file name to a topic")
        void testAddAdditionalValuesToTopic() {
            final String domainName = "de.volkerfaas.arc";
            final String visibilityFullName = "de.volkerfaas.arc.public";
            final Topic topicUserUpdated = new Topic();
            topicUserUpdated.setName("user_updated");

            final Triplet<String, String, Topic> triplet = Triplet.with(domainName, visibilityFullName, topicUserUpdated);
            topologyFileService.addAdditionalValuesToTopic(triplet);
            assertEquals("de.volkerfaas.arc.public.", topicUserUpdated.getPrefix());
            assertEquals("events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.avsc", topicUserUpdated.getValueSchemaFile());
        }

    }

    @Nested
    @DisplayName("the method addAdditionalValuesToVisibility")
    class AddAdditionalValuesToVisibility {

        @Test
        @DisplayName("should add the prefix to a visibility")
        void testAddAdditionalValuesToVisibility() {
            final String domainName = "de.volkerfaas.arc";

            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);

            final Pair<String, Visibility> pair = Pair.with(domainName, visibility);
            final Pair<String, Visibility> result = topologyFileService.addAdditionalValuesToVisibility(pair);
            assertEquals(result, pair);
            assertEquals("de.volkerfaas.arc.", visibility.getPrefix());
        }

    }

    @Nested
    @DisplayName("the method createTopologyIfNotExists")
    class CreateTopologyIfNotExists {

        @Test
        @DisplayName("should return existing topology if it already exists")
        void testNotCreateTopology() {
            final Set<TopologyFile> topologies = new HashSet<>();
            final String pathname = "/topology";
            final String domainName = "de.volkerfaas.arc";

            final TopologyFile topology = new TopologyFile();
            topology.setFile(new File(pathname, "topology-" + domainName + ".yaml"));
            topologies.add(topology);

            final TopologyFile existingTopology = topologyFileService.createTopologyIfNotExists(pathname, domainName, topologies);
            assertNotNull(existingTopology);
            assertEquals(topology, existingTopology);
            assertTrue(topologies.contains(existingTopology));
        }

        @Test
        @DisplayName("should return new topology if it doesn't exist")
        void testCreateTopology() {
            final Set<TopologyFile> topologies = new HashSet<>();
            final String pathname = "/topology";
            final String domainName = "de.volkerfaas.arc";

            final TopologyFile newTopology = topologyFileService.createTopologyIfNotExists(pathname, domainName, topologies);
            assertNotNull(newTopology);
            assertEquals("restore-" + domainName + ".yaml", newTopology.getFile().getName());
            assertTrue(topologies.contains(newTopology));
        }

    }

    @Nested
    @DisplayName("the method createDomainIfNotExists")
    class CreateDomainIfNotExists {

        @Test
        @DisplayName("should return existing domain if it already exists")
        void testNotCreateDomain() {
            final String pathname = "/topology";
            final String domainName = "de.volkerfaas.arc";
            final String domainPrincipal = "User:198753";

            final TopologyFile topology = new TopologyFile();
            topology.setFile(new File(pathname, "topology-" + domainName + ".yaml"));

            final Domain domain = new Domain(domainName, domainPrincipal);
            topology.setDomain(domain);

            final Domain existingDomain = topologyFileService.createDomainIfNotExistsInTopology(domainName, domainPrincipal, topology);
            assertNotNull(existingDomain);
            assertEquals(domain, existingDomain);
            assertEquals(topology.getDomain(), existingDomain);
        }

        @Test
        @DisplayName("should return new domain if it doesn't exist")
        void testCreateDomain() {
            final String pathname = "/topology";
            final String domainName = "de.volkerfaas.arc";
            final String domainPrincipal = "User:198753";

            final TopologyFile topology = new TopologyFile();
            topology.setFile(new File(pathname, "topology-" + domainName + ".yaml"));

            final Domain newDomain = topologyFileService.createDomainIfNotExistsInTopology(domainName, domainPrincipal, topology);
            assertNotNull(newDomain);
            assertEquals(domainName, newDomain.getName());
            assertEquals(domainPrincipal, newDomain.getPrincipal());
            assertEquals(topology.getDomain(), newDomain);
        }

    }

    @Nested
    @DisplayName("the method createVisibilityIfNotExists")
    class CreateVisibilityIfNotExists {

        @Test
        @DisplayName("should return existing visibility if it already exists")
        void testNotCreateVisibility() {
            final String domainName = "de.volkerfaas.arc";
            final String domainPrincipal = "User:198753";
            final Set<String> visibilityPrincipals = Set.of("User:321456", "User:933177");

            final Domain domain = new Domain(domainName, domainPrincipal);

            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.getConsumers().addAll(visibilityPrincipals.stream().map(AccessControl::new).collect(Collectors.toUnmodifiableList()));
            domain.getVisibilities().add(visibility);

            final Visibility existingVisibility = topologyFileService.createVisibilityIfNotExistsInDomain("public", visibilityPrincipals, domain);
            assertNotNull(existingVisibility);
            assertEquals(visibility, existingVisibility);
            assertTrue(domain.getVisibilities().contains(existingVisibility));
        }

        @Test
        @DisplayName("should return new visibility if it doesn't exist")
        void testCreateVisibility() {
            final String domainName = "de.volkerfaas.arc";
            final String domainPrincipal = "User:198753";
            final Set<String> visibilityPrincipals = Set.of("User:321456", "User:933177");

            final Domain domain = new Domain(domainName, domainPrincipal);

            final Visibility newVisibility = topologyFileService.createVisibilityIfNotExistsInDomain("public", visibilityPrincipals, domain);
            assertNotNull(newVisibility);
            assertEquals(Visibility.Type.PUBLIC, newVisibility.getType());
            assertTrue(domain.getVisibilities().contains(newVisibility));
        }

    }

    @Nested
    @DisplayName("the method restoreTopologies")
    class RestoreTopologies {

        @ParameterizedTest
        @ValueSource(strings = {"-key", "-value"})
        @DisplayName("should return a topology matching the cluster configuration")
        void testRestoreTopologies(String suffix) throws ExecutionException, InterruptedException {
            final String domainName = "de.volkerfaas.test";
            final String visibilityType = "public";
            final String fullTopicName = domainName + "." + visibilityType + ".user_updated";
            final Set<String> subjects = mockListSubjects(suffix, fullTopicName);
            mockListTopicsInCluster(fullTopicName);
            mockMethods(domainName, fullTopicName, subjects, suffix);

            final Set<TopologyFile> topologies = topologyFileService.restoreTopologies(topologyDirectory, List.of(domainName));
            assertNotNull(topologies);
            assertEquals(1, topologies.size());
            final TopologyFile topology = topologies.stream().findFirst().orElse(null);
            assertNotNull(topology);
            assertEquals("restore-" + domainName + ".yaml", topology.getFile().getName());
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
        @DisplayName("should return an empty topology if schema registry has no subjects")
        void testNotRestoreTopologiesEmptySchemaRegistry(String suffix) throws ExecutionException, InterruptedException {
            final String domainName = "de.volkerfaas.test";
            final String visibilityType = "public";
            final String fullTopicName = domainName + "." + visibilityType + ".user_updated";
            final Set<String> subjects = Collections.emptySet();
            doReturn(subjects).when(schemaFileService).listSubjects();
            mockListTopicsInCluster(fullTopicName);
            mockMethods(domainName, fullTopicName, subjects, suffix);

            final Set<TopologyFile> topologies = topologyFileService.restoreTopologies(topologyDirectory, List.of(domainName));
            assertNotNull(topologies);
            assertEquals(0, topologies.size());
        }

        @ParameterizedTest
        @ValueSource(strings = {"-key", "-value"})
        @DisplayName("should return an empty topology if schema registry is null")
        void testNotRestoreTopologiesNullSchemaRegistry(String suffix) throws ExecutionException, InterruptedException {
            final String domainName = "de.volkerfaas.test";
            final String visibilityType = "public";
            final String fullTopicName = domainName + "." + visibilityType + ".user_updated";
            doReturn(null).when(schemaFileService).listSubjects();
            mockListTopicsInCluster(fullTopicName);

            doCallRealMethod().when(schemaFileService).findSchema(isNull(), eq(domainName), eq(fullTopicName), eq(suffix));
            doCallRealMethod().when(topicService).createTopic(anyString(), anyCollection(), any(TopicConfiguration.class));
            doAnswer(invocation -> invocation.getArgument(0)).when(topologyFileRepository).writeTopology(any(TopologyFile.class));
            doReturn("").when(schemaFileService).downloadSchemaFile(anyString());

            final Set<TopologyFile> topologies = topologyFileService.restoreTopologies(topologyDirectory, List.of(domainName));
            assertNotNull(topologies);
            assertEquals(0, topologies.size());
        }

        @ParameterizedTest
        @ValueSource(strings = {"-key", "-value"})
        @DisplayName("should return an empty topology if topics in cluster are null")
        void testNotRestoreTopologiesNullTopics(String suffix) throws ExecutionException, InterruptedException {
            final String domainName = "de.volkerfaas.test";
            final String visibilityType = "public";
            final String fullTopicName = domainName + "." + visibilityType + ".user_updated";
            final Set<String> subjects = mockListSubjects(suffix, fullTopicName);
            doReturn(null).when(topicService).listTopicsInCluster();
            mockMethods(domainName, fullTopicName, subjects, suffix);

            final Set<TopologyFile> topologies = topologyFileService.restoreTopologies(topologyDirectory, List.of(domainName));
            assertNotNull(topologies);
            assertEquals(0, topologies.size());
        }

        private void mockMethods(String domainName, String fullTopicName, Set<String> subjects, String suffix) {
            doCallRealMethod().when(schemaFileService).findSchema(eq(subjects), eq(domainName), eq(fullTopicName), eq(suffix));
            doCallRealMethod().when(topicService).createTopic(anyString(), anyCollection(), any(TopicConfiguration.class));
            doAnswer(invocation -> invocation.getArgument(0)).when(topologyFileRepository).writeTopology(any(TopologyFile.class));
            doReturn("").when(schemaFileService).downloadSchemaFile(anyString());
        }

    }

    @Nested
    @DisplayName("the method listDomains")
    class ListDomains {

        @Test
        @DisplayName("should return the domains of all topology files")
        void testListDomains() {
            final TopologyFile topologyFileArc = new TopologyFile();
            final Domain domainArc = new Domain();
            domainArc.setName("de.volkerfaas.arc");
            topologyFileArc.setDomain(domainArc);

            final TopologyFile topologyFileFoo = new TopologyFile();
            final Domain domainFoo = new Domain();
            domainFoo.setName("de.volkerfaas.foo");
            topologyFileFoo.setDomain(domainFoo);

            final TopologyFile topologyFileBar = new TopologyFile();
            final Domain domainBar = new Domain();
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

    @Nested
    @DisplayName("the method skipTopicsNotInEnvironment")
    class SkipTopicsNotInEnvironment {

        @Test
        @DisplayName("should not skip any topic when all topic are assigned to the cluster")
        void testSkipTopicsNotInEnvironment() {
            Collection<TopologyFile> topologies = new ArrayList<>();

            final TopologyFile topologyArc = new TopologyFile();
            final Topic topicUserUpdated = createTopic(topologyArc, "de.volkerfaas.arc", "User:129849", "user_updated", "test");
            topologies.add(topologyArc);

            final TopologyFile topologyFoo = new TopologyFile();
            final Topic topicFooCreated = createTopic(topologyFoo, "de.volkerfaas.foo", "User:123765", "foo_created", "test");
            topologies.add(topologyFoo);

            final TopologyFile topologyBar = new TopologyFile();
            final Topic topicBarDeleted = createTopic(topologyBar, "de.volkerfaas.bar", "User:135918", "bar_deleted", "test");
            topologies.add(topologyBar);

            topologyFileService.skipTopicsNotInEnvironment(topologies, "test");
            final List<Topic> topics = listTopicsInTopologies(topologies);
            assertNotNull(topics);
            assertEquals(3, topics.size());
            assertThat(topics, containsInAnyOrder(topicUserUpdated, topicFooCreated, topicBarDeleted));
        }

        @Test
        @DisplayName("should skip topics that are not assigned to the cluster")
        void testSkipTopicsNotInEnvironmentSkippedOtherEnvironment() {
            Collection<TopologyFile> topologies = new ArrayList<>();

            final TopologyFile topologyArc = new TopologyFile();
            final Topic topicUserUpdated = createTopic(topologyArc, "de.volkerfaas.arc", "User:129849", "user_updated", "test");
            topologies.add(topologyArc);

            final TopologyFile topologyFoo = new TopologyFile();
            final Topic topicFooCreated = createTopic(topologyFoo, "de.volkerfaas.foo", "User:123765", "foo_created", "test");
            topologies.add(topologyFoo);

            final TopologyFile topologyBar = new TopologyFile();
            createTopic(topologyBar, "de.volkerfaas.bar", "User:135918", "bar_deleted", "bar");
            topologies.add(topologyBar);

            topologyFileService.skipTopicsNotInEnvironment(topologies, "test");
            final List<Topic> topics = listTopicsInTopologies(topologies);
            assertNotNull(topics);
            assertEquals(2, topics.size());
            assertThat(topics, containsInAnyOrder(topicUserUpdated, topicFooCreated));
        }

        @Test
        @DisplayName("should skip topics that are not assigned to any cluster")
        void testSkipTopicsNotInEnvironmentSkippedNoEnvironment() {
            Collection<TopologyFile> topologies = new ArrayList<>();

            final TopologyFile topologyArc = new TopologyFile();
            final Topic topicUserUpdated = createTopic(topologyArc, "de.volkerfaas.arc", "User:129849", "user_updated", "test");
            topologies.add(topologyArc);

            final TopologyFile topologyFoo = new TopologyFile();
            createTopic(topologyFoo, "de.volkerfaas.foo", "User:123765", "foo_created", null);
            topologies.add(topologyFoo);

            final TopologyFile topologyBar = new TopologyFile();
            final Topic topicBarDeleted = createTopic(topologyBar, "de.volkerfaas.bar", "User:135918", "bar_deleted", "test");
            topologies.add(topologyBar);

            topologyFileService.skipTopicsNotInEnvironment(topologies, "test");
            final List<Topic> topics = listTopicsInTopologies(topologies);
            assertNotNull(topics);
            assertEquals(2, topics.size());
            assertThat(topics, containsInAnyOrder(topicUserUpdated, topicBarDeleted));
        }

        private Topic createTopic(TopologyFile topology, String domainName, String domainPrincipal, String topicName, String cluster) {
            final Domain domain = new Domain(domainName, domainPrincipal);
            topology.setDomain(domain);
            final Visibility visibility = new Visibility(Visibility.Type.PUBLIC);
            domain.getVisibilities().add(visibility);
            final Topic topic = new Topic(topicName, 4, (short) 3, Collections.emptyMap());
            if (Objects.nonNull(cluster)) topic.getClusters().add(cluster);
            visibility.getTopics().add(topic);

            return topic;
        }

    }

    private List<Topic> listTopicsInTopologies(Collection<TopologyFile> topologies) {
        return topologies.stream()
                .map(TopologyFile::getDomain)
                .map(Domain::getVisibilities)
                .flatMap(List::stream)
                .map(Visibility::getTopics)
                .flatMap(List::stream)
                .collect(Collectors.toUnmodifiableList());
    }

    private Set<String> mockListSubjects(String suffix, String fullTopicName) {
        final String subject = fullTopicName + suffix;
        final Set<String> subjects = Set.of(subject);
        doReturn(subjects).when(schemaFileService).listSubjects();

        return subjects;
    }

    private void mockListTopicsInCluster(String fullTopicName) throws ExecutionException, InterruptedException {
        final Map<String, String> config = new HashMap<>();
        config.put("cleanup.policy", "compact");
        config.put("min.compaction.lag.ms", "100");
        final List<PartitionConfiguration> partitions = List.of(new PartitionConfiguration(fullTopicName, 0), new PartitionConfiguration(fullTopicName, 1), new PartitionConfiguration(fullTopicName, 2), new PartitionConfiguration(fullTopicName, 3));
        final TopicConfiguration topicConfiguration = new TopicConfiguration(fullTopicName, partitions, (short) 1, config);
        doReturn(List.of(topicConfiguration)).when(topicService).listTopicsInCluster();
    }

}
