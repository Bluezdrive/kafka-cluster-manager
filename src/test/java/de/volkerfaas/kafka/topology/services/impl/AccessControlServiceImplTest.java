package de.volkerfaas.kafka.topology.services.impl;

import de.volkerfaas.kafka.cluster.model.ClusterConfiguration;
import de.volkerfaas.kafka.cluster.repositories.impl.KafkaClusterRepositoryImpl;
import de.volkerfaas.kafka.topology.model.*;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@DisplayName("In the class AccessControlServiceImpl")
class AccessControlServiceImplTest {

    private KafkaClusterRepositoryImpl kafkaClusterRepository;
    private AccessControlServiceImpl accessControlService;

    @BeforeAll
    static void setup() {

    }

    @BeforeEach
    void init() {
        this.kafkaClusterRepository = mock(KafkaClusterRepositoryImpl.class);
        this.accessControlService = new AccessControlServiceImpl(kafkaClusterRepository);
    }

    @Nested
    @DisplayName("the method listOrphanedAclBindingFilters")
    class ListOrphanedAclBindingFilters {

        @Test
        @DisplayName("should return a set of orphaned AclBindingFilter when principal is removed from visibility")
        void testOrphanedVisibilityAclBindings() throws ExecutionException, InterruptedException {
            final String resourceName = "de.volkerfaas.arc.public.";
            final String principal = "User:129849";
            final boolean prefix = true;
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("");
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(resourceName, principal, prefix));

            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");

            final Set<AclBindingFilter> orphanedAclBindings = accessControlService.listOrphanedAclBindingFilters(visibility, prefix);
            assertEquals(3, orphanedAclBindings.size());
            assertThat(orphanedAclBindings, containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.DESCRIBE, prefix).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.READ, prefix).toFilter(),
                    accessControlService.getAclBinding(ResourceType.GROUP, resourceName, principal, AclOperation.READ, prefix).toFilter()
            ));
        }

        @Test
        @DisplayName("should return a set of  orphaned AclBindingFilter when principal is removed from topic")
        void testOrphanedTopicAclBindings() throws ExecutionException, InterruptedException {
            final String resourceName = "de.volkerfaas.arc.public.user_updated";
            final String principal = "User:129849";
            final boolean prefix = false;
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("");
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(resourceName, principal, prefix));

            final Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setPrefix("de.volkerfaas.arc.public.");

            final Set<AclBindingFilter> orphanedAclBindings = accessControlService.listOrphanedAclBindingFilters(topic, prefix);
            assertEquals(3, orphanedAclBindings.size());
            assertThat(orphanedAclBindings, containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.DESCRIBE, prefix).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.READ, prefix).toFilter(),
                    accessControlService.getAclBinding(ResourceType.GROUP, resourceName, principal, AclOperation.READ, prefix).toFilter()
            ));
        }

        @Test
        @DisplayName("should return no orphaned AclBindingFilter when principal is set in visibility")
        void testNoOrphanedVisibilityAclBindings() throws ExecutionException, InterruptedException {
            final String principal = "User:129849";
            final String resourceName = "de.volkerfaas.arc.public.";
            final boolean prefix = true;
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("");
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(resourceName, principal, prefix));

            final AccessControl consumer = new AccessControl();
            consumer.setPrincipal(principal);

            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            visibility.getConsumers().add(consumer);

            final Set<AclBindingFilter> orphanedAclBindingFilters = accessControlService.listOrphanedAclBindingFilters(visibility, prefix);
            assertEquals(0, orphanedAclBindingFilters.size());
        }

        @Test
        @DisplayName("should return no orphaned AclBindingFilter when principal is set in domain")
        void testNoOrphanedDomainAclBindings() throws ExecutionException, InterruptedException {
            final String domainName = "de.volkerfaas.arc";
            final String principal = "User:129849";
            final String resourceName = domainName + ".";

            final Domain domain = new Domain();
            domain.setName(domainName);
            domain.setPrincipal(principal);

            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("");
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(resourceName, principal, true));
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForProducer(resourceName, principal));

            final Set<AclBindingFilter> orphanedAclBindingFilters = accessControlService.listOrphanedAclBindingFilters(domain);
            assertEquals(0, orphanedAclBindingFilters.size());
        }

        @Test
        @DisplayName("should return a set of  orphaned AclBindingFilter when principal is changed in domain")
        void testOrphanedDomainAclBindings() throws ExecutionException, InterruptedException {
            final String domainName = "de.volkerfaas.arc";
            final String principal = "User:129849";
            final String oldPrincipal = "User:129933";
            final String resourceName = domainName + ".";
            final String otherPrincipal = "User:130777";
            final String otherResourceName = "de.volkerfaas.test.";

            final Domain domain = new Domain();
            domain.setName(domainName);
            domain.setPrincipal(principal);

            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("");
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(resourceName, oldPrincipal, true));
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForProducer(resourceName, oldPrincipal));
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(otherResourceName, otherPrincipal, true));
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForProducer(otherResourceName, otherPrincipal));

            final Set<AclBindingFilter> orphanedAclBindingFilters = accessControlService.listOrphanedAclBindingFilters(domain);
            assertEquals(6, orphanedAclBindingFilters.size());
            assertThat(orphanedAclBindingFilters, containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, oldPrincipal, AclOperation.DESCRIBE, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, oldPrincipal, AclOperation.READ, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.GROUP, resourceName, oldPrincipal, AclOperation.READ, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, oldPrincipal, AclOperation.WRITE, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TRANSACTIONAL_ID, resourceName, oldPrincipal, AclOperation.WRITE, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.CLUSTER, "kafka-cluster", oldPrincipal, AclOperation.IDEMPOTENT_WRITE, false).toFilter()
            ));

            assertThat(orphanedAclBindingFilters, not(containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, otherResourceName, otherPrincipal, AclOperation.DESCRIBE, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TOPIC, otherResourceName, otherPrincipal, AclOperation.READ, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.GROUP, otherResourceName, otherPrincipal, AclOperation.READ, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TOPIC, otherResourceName, otherPrincipal, AclOperation.WRITE, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TRANSACTIONAL_ID, otherResourceName, otherPrincipal, AclOperation.WRITE, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.CLUSTER, "kafka-cluster", otherPrincipal, AclOperation.IDEMPOTENT_WRITE, false).toFilter()
            )));
        }

    }

    @Nested
    @DisplayName("the method listAclBindingFiltersNotInDomains")
    class ListAclBindingFiltersNotInDomains {

        @Test
        @DisplayName("should return a set of orphaned AclBindingFilter when domain is removed")
        void testOrphanedDomainAclBindings() throws ExecutionException, InterruptedException {
            final String domainNameArc = "de.volkerfaas.arc";
            final String principalArc = "User:129849";
            final String resourceNameArc = domainNameArc + ".";
            final String domainNameTest = "de.volkerfaas.test";
            final String principalTest = "User:138166";
            final String resourceNameTest = domainNameTest + ".";

            final String domainNameRemoved = "de.volkerfaas.other";
            final String principalRemoved = "User:130777";
            final String resourceNameRemoved = domainNameRemoved + ".";

            final List<Domain> domains = List.of(
                    new Domain(domainNameArc, principalArc),
                    new Domain(domainNameTest, principalTest)
            );

            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("");
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(resourceNameArc, principalArc, true));
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForProducer(resourceNameArc, principalArc));
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(resourceNameTest, principalTest, true));
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForProducer(resourceNameTest, principalTest));
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(resourceNameRemoved, principalRemoved, true));
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForProducer(resourceNameRemoved, principalRemoved));

            final Set<AclBindingFilter> orphanedAclBindingFilters = accessControlService.listAclBindingFiltersNotInDomains(domains);
            assertEquals(6, orphanedAclBindingFilters.size());
            assertThat(orphanedAclBindingFilters, containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceNameRemoved, principalRemoved, AclOperation.DESCRIBE, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceNameRemoved, principalRemoved, AclOperation.READ, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.GROUP, resourceNameRemoved, principalRemoved, AclOperation.READ, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceNameRemoved, principalRemoved, AclOperation.WRITE, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TRANSACTIONAL_ID, resourceNameRemoved, principalRemoved, AclOperation.WRITE, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.CLUSTER, "kafka-cluster", principalRemoved, AclOperation.IDEMPOTENT_WRITE, false).toFilter()
            ));

            assertThat(orphanedAclBindingFilters, not(containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceNameArc, principalArc, AclOperation.DESCRIBE, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceNameArc, principalArc, AclOperation.READ, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.GROUP, resourceNameArc, principalArc, AclOperation.READ, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceNameArc, principalArc, AclOperation.WRITE, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TRANSACTIONAL_ID, resourceNameArc, principalArc, AclOperation.WRITE, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.CLUSTER, "kafka-cluster", principalArc, AclOperation.IDEMPOTENT_WRITE, false).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceNameTest, principalTest, AclOperation.DESCRIBE, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceNameTest, principalTest, AclOperation.READ, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.GROUP, resourceNameTest, principalTest, AclOperation.READ, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceNameTest, principalTest, AclOperation.WRITE, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TRANSACTIONAL_ID, resourceNameTest, principalTest, AclOperation.WRITE, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.CLUSTER, "kafka-cluster", principalTest, AclOperation.IDEMPOTENT_WRITE, false).toFilter()
            )));
        }

    }

    @Nested
    @DisplayName("the method listAclBindingsForDomain")
    class ListAclBindingsForDomain {

        @Test
        @DisplayName("should return a set of AclBinding for the domain")
        void testListDomainAclBindings() throws ExecutionException, InterruptedException {
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("");
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

            final Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            domain.setPrincipal("User:129849");

            final Set<AclBinding> domainAclBindings = accessControlService.listAclBindingsForDomain(domain);
            assertEquals(6, domainAclBindings.size());
            final String resourceName = domain.getName() + ".";
            final String principal = domain.getPrincipal();
            assertThat(domainAclBindings, containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.DESCRIBE, true),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.READ, true),
                    accessControlService.getAclBinding(ResourceType.GROUP, resourceName, principal, AclOperation.READ, true),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.WRITE, true),
                    accessControlService.getAclBinding(ResourceType.TRANSACTIONAL_ID, resourceName, principal, AclOperation.WRITE, true),
                    accessControlService.getAclBinding(ResourceType.CLUSTER, "kafka-cluster", principal, AclOperation.IDEMPOTENT_WRITE, false)
            ));
        }

    }

    @Nested
    @DisplayName("the method listAclBindingsForVisibilityOrTopic")
    class ListAclBindingsForVisibilityOrTopic {

        @Test
        @DisplayName("should return a set of AclBinding for a visibility")
        void testListVisibilityAclBindings() throws ExecutionException, InterruptedException {
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("");
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");

            final String principal = "User:129849";
            final AccessControl accessControl = new AccessControl();
            accessControl.setPrincipal(principal);
            visibility.getConsumers().add(accessControl);

            final Set<AclBinding> visibilityAclBindings = accessControlService.listAclBindingsForVisibilityOrTopic(visibility, true);
            assertEquals(3, visibilityAclBindings.size());
            final String resourceName = visibility.getFullName() + ".";
            assertThat(visibilityAclBindings, containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.DESCRIBE, true),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.READ, true),
                    accessControlService.getAclBinding(ResourceType.GROUP, resourceName, principal, AclOperation.READ, true)
            ));
        }

        @Test
        @DisplayName("should return a set of AclBinding for a topic")
        void testListTopicAclBindings() throws ExecutionException, InterruptedException {
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("");
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

            final Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setPrefix("de.volkerfaas.arc.public.");

            final String principal = "User:129849";
            final AccessControl accessControl = new AccessControl();
            accessControl.setPrincipal(principal);
            topic.getConsumers().add(accessControl);

            final Set<AclBinding> visibilityAclBindings = accessControlService.listAclBindingsForVisibilityOrTopic(topic, false);
            assertEquals(3, visibilityAclBindings.size());
            final String resourceName = topic.getFullName();
            assertThat(visibilityAclBindings, containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.DESCRIBE, false),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.READ, false),
                    accessControlService.getAclBinding(ResourceType.GROUP, resourceName, principal, AclOperation.READ, false)
            ));
        }

    }

    @Nested
    @DisplayName("the method listAclBindingsForConsumer")
    class ListAclBindingsForConsumer {

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        @DisplayName("should return a set of AclBinding for a consumer")
        void testListConsumerAclBindings(boolean prefix) throws ExecutionException, InterruptedException {
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("");
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

            final String resourceName = "de.volkerfaas.arc.public.";
            final String principal = "User:129849";

            final Set<AclBinding> consumerAclBindings = accessControlService.listAclBindingsForConsumer(resourceName, principal, prefix);
            assertEquals(3, consumerAclBindings.size());
            assertThat(consumerAclBindings, containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.DESCRIBE, prefix),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.READ, prefix),
                    accessControlService.getAclBinding(ResourceType.GROUP, resourceName, principal, AclOperation.READ, prefix)
            ));
        }

    }

    @Nested
    @DisplayName("the method listAclBindingsForProducer")
    class ListAclBindingsForProducer {

        @Test
        @DisplayName("should return a set of AclBinding for a producer")
        void testListProducerAclBindings() throws ExecutionException, InterruptedException {
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("");
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

            final String resourceName = "de.volkerfaas.arc.";
            final String principal = "User:129849";

            final Set<AclBinding> consumerAclBindings = accessControlService.listAclBindingsForProducer(resourceName, principal);
            assertEquals(3, consumerAclBindings.size());
            assertThat(consumerAclBindings, containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.WRITE, true),
                    accessControlService.getAclBinding(ResourceType.TRANSACTIONAL_ID, resourceName, principal, AclOperation.WRITE, true),
                    accessControlService.getAclBinding(ResourceType.CLUSTER, "kafka-cluster", principal, AclOperation.IDEMPOTENT_WRITE, false)
            ));
        }

    }

    @Nested
    @DisplayName("the method listPrincipals")
    class ListPrincipals {

        @Test
        @DisplayName("should return a set of principals for a visibility")
        void testListPrincipals() {
            final Visibility visibility = new Visibility();
            final AccessControl accessControlFirst = new AccessControl();
            accessControlFirst.setPrincipal("User:129849");
            final AccessControl accessControlSecond = new AccessControl();
            accessControlSecond.setPrincipal("User:129931");
            visibility.getConsumers().addAll(List.of(accessControlFirst, accessControlSecond));

            final Set<String> consumerPrincipals = accessControlService.listPrincipals(visibility);
            assertEquals(2, consumerPrincipals.size());
            assertThat(consumerPrincipals, hasItems("User:129849", "User:129931"));
        }

    }

    @Nested
    @DisplayName("the method getResourceName")
    class GetResourceName {

        @Test
        @DisplayName("should return the full name for a visibility followed by a dot")
        void testGetVisibilityResourceName() {
            final Visibility visibility = new Visibility();
            visibility.setPrefix("de.volkerfaas.arc.");
            visibility.setType(Visibility.Type.PUBLIC);

            final String resourceName = accessControlService.getResourceName(visibility, true);
            assertNotNull(resourceName);
            assertEquals("de.volkerfaas.arc.public.", resourceName);
        }

        @Test
        @DisplayName("should return the full name for a topic not followed by a dot")
        void testGetTopicResourceName() {
            final Topic topic = new Topic();
            topic.setPrefix("de.volkerfaas.arc.public.");
            topic.setName("user_updated");

            final String resourceName = accessControlService.getResourceName(topic, false);
            assertNotNull(resourceName);
            assertEquals("de.volkerfaas.arc.public.user_updated", resourceName);
        }

    }

    @Nested
    @DisplayName("the method isAclNotAvailable")
    class IsAclNotAvailable {

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        @DisplayName("should return true when the specified acl is not available in the given list of acls")
        void testAclIsNotInList(boolean prefix) {
            final Collection<AclBinding> aclBindings = Set.of(
                    accessControlService.getAclBinding(ResourceType.TOPIC, "de.volkerfaas.arc.", "User:125382", AclOperation.WRITE, prefix),
                    accessControlService.getAclBinding(ResourceType.TOPIC, "de.volkerfaas.arc.", "User:125382", AclOperation.READ, prefix),
                    accessControlService.getAclBinding(ResourceType.GROUP, "de.volkerfaas.arc.", "User:125382", AclOperation.READ, prefix)
            );
            final AclBinding aclBinding = accessControlService.getAclBinding(ResourceType.TRANSACTIONAL_ID, "de.volkerfaas.arc.", "User:125382", AclOperation.WRITE, prefix);
            final boolean aclNotAvailable = accessControlService.isAclNotAvailable(aclBindings, aclBinding);
            assertTrue(aclNotAvailable);
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        @DisplayName("should return false when the specified acl is available in the given list of acls")
        void testAclIsInList(boolean prefix) {
            final Collection<AclBinding> aclBindings = Set.of(
                    accessControlService.getAclBinding(ResourceType.TOPIC, "de.volkerfaas.arc.", "User:125382", AclOperation.WRITE, prefix),
                    accessControlService.getAclBinding(ResourceType.TOPIC, "de.volkerfaas.arc.", "User:125382", AclOperation.READ, prefix),
                    accessControlService.getAclBinding(ResourceType.GROUP, "de.volkerfaas.arc.", "User:125382", AclOperation.READ, prefix),
                    accessControlService.getAclBinding(ResourceType.TRANSACTIONAL_ID, "de.volkerfaas.arc.", "User:125382", AclOperation.WRITE, prefix)
            );
            final AclBinding aclBinding = accessControlService.getAclBinding(ResourceType.TRANSACTIONAL_ID, "de.volkerfaas.arc.", "User:125382", AclOperation.WRITE, prefix);
            final boolean aclNotAvailable = accessControlService.isAclNotAvailable(aclBindings, aclBinding);
            assertFalse(aclNotAvailable);
        }

    }

    @Nested
    @DisplayName("the method listNewAclBindings")
    class ListNewAclBindings {

        @Test
        @DisplayName("should return all new acls for a domain when they don't exist in the cluster")
        void testNewAclBindings() throws ExecutionException, InterruptedException {
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("");
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

            final TopologyFile topology = new TopologyFile();

            final Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            domain.setPrincipal("User:129849");
            topology.setDomain(domain);

            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            visibility.getConsumers().add(new AccessControl("User:129850"));
            domain.getVisibilities().add(visibility);

            final Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setNumPartitions(5);
            topic.setReplicationFactor((short) 2);
            topic.setPrefix("de.volkerfaas.arc.public.");
            topic.getConsumers().add(new AccessControl("User:129851"));
            visibility.getTopics().add(topic);

            final Collection<AclBinding> newAclBindings = accessControlService.listNewAclBindings(List.of(domain));
            assertNotNull(newAclBindings);
            assertEquals(12, newAclBindings.size());
            assertThat(newAclBindings, containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, "de.volkerfaas.arc.", "User:129849", AclOperation.WRITE, true),
                    accessControlService.getAclBinding(ResourceType.TRANSACTIONAL_ID, "de.volkerfaas.arc.", "User:129849", AclOperation.WRITE, true),
                    accessControlService.getAclBinding(ResourceType.CLUSTER, "kafka-cluster", "User:129849", AclOperation.IDEMPOTENT_WRITE, false),
                    accessControlService.getAclBinding(ResourceType.TOPIC, "de.volkerfaas.arc.", "User:129849", AclOperation.DESCRIBE, true),
                    accessControlService.getAclBinding(ResourceType.TOPIC, "de.volkerfaas.arc.", "User:129849", AclOperation.READ, true),
                    accessControlService.getAclBinding(ResourceType.GROUP, "de.volkerfaas.arc.", "User:129849", AclOperation.READ, true),
                    accessControlService.getAclBinding(ResourceType.TOPIC, "de.volkerfaas.arc.public.", "User:129850", AclOperation.DESCRIBE, true),
                    accessControlService.getAclBinding(ResourceType.TOPIC, "de.volkerfaas.arc.public.", "User:129850", AclOperation.READ, true),
                    accessControlService.getAclBinding(ResourceType.GROUP, "de.volkerfaas.arc.public.", "User:129850", AclOperation.READ, true),
                    accessControlService.getAclBinding(ResourceType.TOPIC, "de.volkerfaas.arc.public.user_updated", "User:129851", AclOperation.DESCRIBE, false),
                    accessControlService.getAclBinding(ResourceType.TOPIC, "de.volkerfaas.arc.public.user_updated", "User:129851", AclOperation.READ, false),
                    accessControlService.getAclBinding(ResourceType.GROUP, "de.volkerfaas.arc.public.user_updated", "User:129851", AclOperation.READ, false)
            ));
        }

    }

    @Nested
    @DisplayName("the method listOrphanedAclBindings")
    class ListOrphanedAclBindings {

        @Test
        @DisplayName("should return all orphaned acls when a domain is removed, but ignore all other acls")
        void testOrphanedAclBindings() throws ExecutionException, InterruptedException {
            final String domainNameArc = "de.volkerfaas.arc";
            final String principalArc = "User:129849";
            final String resourceNameArc = domainNameArc + ".";
            final String domainNameTest = "de.volkerfaas.test";
            final String principalTest = "User:138166";
            final String resourceNameTest = domainNameTest + ".";

            final String domainNameRemoved = "de.volkerfaas.other";
            final String principalRemoved = "User:130777";
            final String resourceNameRemoved = domainNameRemoved + ".";

            final List<Domain> domains = List.of(
                    new Domain(domainNameArc, principalArc),
                    new Domain(domainNameTest, principalTest)
            );

            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("");
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(resourceNameArc, principalArc, true));
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForProducer(resourceNameArc, principalArc));
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(resourceNameTest, principalTest, true));
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForProducer(resourceNameTest, principalTest));
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(resourceNameRemoved, principalRemoved, true));
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForProducer(resourceNameRemoved, principalRemoved));

            final Collection<AclBindingFilter> orphanedAclBindingFilters = accessControlService.listOrphanedAclBindings(domains);
            assertNotNull(orphanedAclBindingFilters);
            assertThat(orphanedAclBindingFilters, containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceNameRemoved, principalRemoved, AclOperation.DESCRIBE, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceNameRemoved, principalRemoved, AclOperation.READ, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.GROUP, resourceNameRemoved, principalRemoved, AclOperation.READ, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceNameRemoved, principalRemoved, AclOperation.WRITE, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TRANSACTIONAL_ID, resourceNameRemoved, principalRemoved, AclOperation.WRITE, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.CLUSTER, "kafka-cluster", principalRemoved, AclOperation.IDEMPOTENT_WRITE, false).toFilter()
            ));

            assertThat(orphanedAclBindingFilters, not(containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceNameArc, principalArc, AclOperation.DESCRIBE, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceNameArc, principalArc, AclOperation.READ, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.GROUP, resourceNameArc, principalArc, AclOperation.READ, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceNameArc, principalArc, AclOperation.WRITE, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TRANSACTIONAL_ID, resourceNameArc, principalArc, AclOperation.WRITE, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.CLUSTER, "kafka-cluster", principalArc, AclOperation.IDEMPOTENT_WRITE, false).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceNameTest, principalTest, AclOperation.DESCRIBE, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceNameTest, principalTest, AclOperation.READ, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.GROUP, resourceNameTest, principalTest, AclOperation.READ, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceNameTest, principalTest, AclOperation.WRITE, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TRANSACTIONAL_ID, resourceNameTest, principalTest, AclOperation.WRITE, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.CLUSTER, "kafka-cluster", principalTest, AclOperation.IDEMPOTENT_WRITE, false).toFilter()
            )));
        }

    }

}
