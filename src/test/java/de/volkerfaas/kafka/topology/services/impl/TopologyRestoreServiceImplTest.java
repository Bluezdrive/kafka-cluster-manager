package de.volkerfaas.kafka.topology.services.impl;

import de.volkerfaas.kafka.cluster.model.PartitionConfiguration;
import de.volkerfaas.kafka.cluster.model.TopicConfiguration;
import de.volkerfaas.kafka.topology.model.*;
import de.volkerfaas.kafka.topology.repositories.TopologyFileRepository;
import de.volkerfaas.kafka.topology.services.AccessControlService;
import de.volkerfaas.kafka.topology.services.TopologyValuesService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@DisplayName("In the class TopologyRestoreServiceImpl")
public class TopologyRestoreServiceImplTest {

    private SchemaFileServiceImpl schemaFileService;
    private TopologyRestoreServiceImpl topologyRestoreService;
    private TopologyFileRepository topologyFileRepository;
    private TopicServiceImpl topicService;
    private String topologyDirectory;

    @BeforeEach
    void init() {
        final URL resource = getClass().getClassLoader().getResource("topology-de.volkerfaas.arc.yaml");
        assertNotNull(resource);
        File topologyFile = new File(resource.getPath());
        this.topologyDirectory = topologyFile.getParent();

        final AccessControlService accessControlService = mock(AccessControlService.class);
        this.schemaFileService = mock(SchemaFileServiceImpl.class);
        this.topicService = mock(TopicServiceImpl.class);
        this.topologyFileRepository = mock(TopologyFileRepository.class);
        final TopologyValuesService topologyValuesService = new TopologyValuesServiceImpl();
        this.topologyRestoreService = new TopologyRestoreServiceImpl(accessControlService, schemaFileService, topicService, topologyFileRepository, topologyValuesService);
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
            topology.setFile(new File(pathname, "restore-" + domainName + ".yaml"));
            topologies.add(topology);

            final TopologyFile existingTopology = topologyRestoreService.createTopologyIfNotExists(pathname, domainName, topologies);
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

            final TopologyFile newTopology = topologyRestoreService.createTopologyIfNotExists(pathname, domainName, topologies);
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

            final Domain existingDomain = topologyRestoreService.createDomainIfNotExistsInTopology(domainName, domainPrincipal, topology);
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

            final Domain newDomain = topologyRestoreService.createDomainIfNotExistsInTopology(domainName, domainPrincipal, topology);
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

            final Visibility existingVisibility = topologyRestoreService.createVisibilityIfNotExistsInDomain("public", visibilityPrincipals, domain);
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

            final Visibility newVisibility = topologyRestoreService.createVisibilityIfNotExistsInDomain("public", visibilityPrincipals, domain);
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
        void testRestoreTopologies(String suffix) throws ExecutionException, InterruptedException, IOException, RestClientException {
            final String domainName = "de.volkerfaas.test";
            final String visibilityType = "public";
            final String fullTopicName = domainName + "." + visibilityType + ".user_updated";
            final Set<String> subjects = mockListSubjects(suffix, fullTopicName);
            mockListTopicsInCluster(fullTopicName);
            mockMethods(domainName, fullTopicName, subjects, suffix);

            final Set<TopologyFile> topologies = topologyRestoreService.restoreTopologies(topologyDirectory, List.of(domainName));
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
        void testNotRestoreTopologiesEmptySchemaRegistry(String suffix) throws ExecutionException, InterruptedException, IOException, RestClientException {
            final String domainName = "de.volkerfaas.test";
            final String visibilityType = "public";
            final String fullTopicName = domainName + "." + visibilityType + ".user_updated";
            final Set<String> subjects = Collections.emptySet();
            doReturn(subjects).when(schemaFileService).listSubjects();
            mockListTopicsInCluster(fullTopicName);
            mockMethods(domainName, fullTopicName, subjects, suffix);

            final Set<TopologyFile> topologies = topologyRestoreService.restoreTopologies(topologyDirectory, List.of(domainName));
            assertNotNull(topologies);
            assertEquals(0, topologies.size());
        }

        @ParameterizedTest
        @ValueSource(strings = {"-key", "-value"})
        @DisplayName("should return an empty topology if schema registry is null")
        void testNotRestoreTopologiesNullSchemaRegistry(String suffix) throws ExecutionException, InterruptedException, IOException, RestClientException {
            final String domainName = "de.volkerfaas.test";
            final String visibilityType = "public";
            final String fullTopicName = domainName + "." + visibilityType + ".user_updated";
            doReturn(null).when(schemaFileService).listSubjects();
            mockListTopicsInCluster(fullTopicName);

            doCallRealMethod().when(schemaFileService).findSchema(isNull(), eq(domainName), eq(fullTopicName), eq(suffix));
            doCallRealMethod().when(topicService).createTopic(anyString(), anyCollection(), any(TopicConfiguration.class));
            doAnswer(invocation -> invocation.getArgument(0)).when(topologyFileRepository).writeTopology(any(TopologyFile.class));
            doNothing().when(schemaFileService).downloadSchemas(anyCollection(), eq(topologyDirectory));

            final Set<TopologyFile> topologies = topologyRestoreService.restoreTopologies(topologyDirectory, List.of(domainName));
            assertNotNull(topologies);
            assertEquals(0, topologies.size());
        }

        @ParameterizedTest
        @ValueSource(strings = {"-key", "-value"})
        @DisplayName("should return an empty topology if topics in cluster are null")
        void testNotRestoreTopologiesNullTopics(String suffix) throws ExecutionException, InterruptedException, IOException, RestClientException {
            final String domainName = "de.volkerfaas.test";
            final String visibilityType = "public";
            final String fullTopicName = domainName + "." + visibilityType + ".user_updated";
            final Set<String> subjects = mockListSubjects(suffix, fullTopicName);
            doReturn(null).when(topicService).listTopicsInCluster();
            mockMethods(domainName, fullTopicName, subjects, suffix);

            final Set<TopologyFile> topologies = topologyRestoreService.restoreTopologies(topologyDirectory, List.of(domainName));
            assertNotNull(topologies);
            assertEquals(0, topologies.size());
        }

        private void mockMethods(String domainName, String fullTopicName, Set<String> subjects, String suffix) throws IOException, RestClientException {
            doCallRealMethod().when(schemaFileService).findSchema(eq(subjects), eq(domainName), eq(fullTopicName), eq(suffix));
            doCallRealMethod().when(topicService).createTopic(anyString(), anyCollection(), any(TopicConfiguration.class));
            doAnswer(invocation -> invocation.getArgument(0)).when(topologyFileRepository).writeTopology(any(TopologyFile.class));
            doNothing().when(schemaFileService).downloadSchemas(anyCollection(), eq(topologyDirectory));
        }

    }

    private Set<String> mockListSubjects(String suffix, String fullTopicName) throws IOException, RestClientException {
        final String subject = fullTopicName + suffix;
        final Set<String> subjects = Set.of(subject);
        doReturn(subjects).when(schemaFileService).listSubjects();
        doReturn(Schema.CompatibilityMode.FORWARD_TRANSITIVE).when(schemaFileService).getCompatibility(anyString());

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
