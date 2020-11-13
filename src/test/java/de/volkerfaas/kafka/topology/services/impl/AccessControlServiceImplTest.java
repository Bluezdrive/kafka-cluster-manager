package de.volkerfaas.kafka.topology.services.impl;

import de.volkerfaas.kafka.topology.model.*;
import de.volkerfaas.kafka.topology.repositories.impl.KafkaClusterRepositoryImpl;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

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

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class AccessControlServiceImplTest {

    private KafkaClusterRepositoryImpl kafkaClusterRepository;
    private AccessControlServiceImpl accessControlService;

    @BeforeAll
    static void setup() {

    }

    @BeforeEach
    void init() {
        this.kafkaClusterRepository = Mockito.mock(KafkaClusterRepositoryImpl.class);
        this.accessControlService = new AccessControlServiceImpl(kafkaClusterRepository);
    }

    @Nested
    class listOrphanedAclBindingFilters {

        @Test
        void returns_orphaned_acl_bindings_when_principal_is_removed_from_visibility() throws ExecutionException, InterruptedException {
            final String resourceName = "de.volkerfaas.arc.public.";
            final String principal = "User:129849";
            final boolean prefix = true;
            final KafkaCluster kafkaCluster = new KafkaCluster("");
            doReturn(kafkaCluster).when(kafkaClusterRepository).dumpCluster();
            kafkaCluster.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(resourceName, principal, prefix));

            Visibility visibility = new Visibility();
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
        void returns_orphaned_acl_bindings_when_principal_is_removed_from_topic() throws ExecutionException, InterruptedException {
            final String resourceName = "de.volkerfaas.arc.public.user_updated";
            final String principal = "User:129849";
            final boolean prefix = false;
            final KafkaCluster kafkaCluster = new KafkaCluster("");
            doReturn(kafkaCluster).when(kafkaClusterRepository).dumpCluster();
            kafkaCluster.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(resourceName, principal, prefix));

            Topic topic = new Topic();
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
        void returns_no_orphaned_acl_bindings_when_principal_set_in_visibility() throws ExecutionException, InterruptedException {
            final String principal = "User:129849";
            final String resourceName = "de.volkerfaas.arc.public.";
            final boolean prefix = true;
            final KafkaCluster kafkaCluster = new KafkaCluster("");
            doReturn(kafkaCluster).when(kafkaClusterRepository).dumpCluster();
            kafkaCluster.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(resourceName, principal, prefix));

            AccessControl consumer = new AccessControl();
            consumer.setPrincipal(principal);

            Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            visibility.getConsumers().add(consumer);

            final Set<AclBindingFilter> orphanedAclBindingFilters = accessControlService.listOrphanedAclBindingFilters(visibility, prefix);
            assertEquals(0, orphanedAclBindingFilters.size());
        }

        @Test
        void returns_no_orphaned_acl_bindings_when_principal_set_in_domain() throws ExecutionException, InterruptedException {
            final String domainName = "de.volkerfaas.arc";
            final String principal = "User:129849";
            final String resourceName = domainName + ".";

            final Domain domain = new Domain();
            domain.setName(domainName);
            domain.setPrincipal(principal);

            final KafkaCluster kafkaCluster = new KafkaCluster("");
            doReturn(kafkaCluster).when(kafkaClusterRepository).dumpCluster();
            kafkaCluster.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(resourceName, principal, true));
            kafkaCluster.getAclBindings().addAll(accessControlService.listAclBindingsForProducer(resourceName, principal));

            final Set<AclBindingFilter> orphanedAclBindingFilters = accessControlService.listOrphanedAclBindingFilters(domain);
            assertEquals(0, orphanedAclBindingFilters.size());
        }

        @Test
        void returns_orphaned_acl_bindings_when_principal_is_changed_in_domain() throws ExecutionException, InterruptedException {
            final String domainName = "de.volkerfaas.arc";
            final String principal = "User:129849";
            final String oldPrincipal = "User:129933";
            final String resourceName = domainName + ".";
            final String otherPrincipal = "User:130777";
            final String otherResourceName = "de.volkerfaas.test.";

            final Domain domain = new Domain();
            domain.setName(domainName);
            domain.setPrincipal(principal);

            final KafkaCluster kafkaCluster = new KafkaCluster("");
            doReturn(kafkaCluster).when(kafkaClusterRepository).dumpCluster();
            kafkaCluster.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(resourceName, oldPrincipal, true));
            kafkaCluster.getAclBindings().addAll(accessControlService.listAclBindingsForProducer(resourceName, oldPrincipal));
            kafkaCluster.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(otherResourceName, otherPrincipal, true));
            kafkaCluster.getAclBindings().addAll(accessControlService.listAclBindingsForProducer(otherResourceName, otherPrincipal));

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
    class listAclBindingFiltersNotInDomains {

        @Test
        void returns_orphaned_acl_bindings_when_domain_is_removed() throws ExecutionException, InterruptedException {
            final String domainNameArc = "de.volkerfaas.arc";
            final String principalArc = "User:129849";
            final String resourceNameArc = domainNameArc + ".";
            final String domainNameTest = "de.volkerfaas.test";
            final String principalTest = "User:138166";
            final String resourceNameTest = domainNameTest + ".";

            final String domainNameRemoved = "de.volkerfaas.other";
            final String principalRemoved = "User:130777";
            final String resourceNameRemoved = domainNameRemoved + ".";

            List<Domain> domains = List.of(
                    new Domain(domainNameArc, principalArc),
                    new Domain(domainNameTest, principalTest)
            );

            final KafkaCluster kafkaCluster = new KafkaCluster("");
            doReturn(kafkaCluster).when(kafkaClusterRepository).dumpCluster();
            kafkaCluster.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(resourceNameArc, principalArc, true));
            kafkaCluster.getAclBindings().addAll(accessControlService.listAclBindingsForProducer(resourceNameArc, principalArc));
            kafkaCluster.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(resourceNameTest, principalTest, true));
            kafkaCluster.getAclBindings().addAll(accessControlService.listAclBindingsForProducer(resourceNameTest, principalTest));
            kafkaCluster.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(resourceNameRemoved, principalRemoved, true));
            kafkaCluster.getAclBindings().addAll(accessControlService.listAclBindingsForProducer(resourceNameRemoved, principalRemoved));

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
    class listAclBindingsForDomain {

        @Test
        void returns_a_list_with_all_acl_bindings_for_domain() throws ExecutionException, InterruptedException {
            final KafkaCluster kafkaCluster = new KafkaCluster("");
            doReturn(kafkaCluster).when(kafkaClusterRepository).dumpCluster();

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
    class listAclBindingsForVisibilityOrTopic {

        @Test
        void returns_a_list_with_all_acl_bindings_for_visibility() throws ExecutionException, InterruptedException {
            final KafkaCluster kafkaCluster = new KafkaCluster("");
            doReturn(kafkaCluster).when(kafkaClusterRepository).dumpCluster();

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
        void returns_a_list_with_all_acl_bindings_for_topic() throws ExecutionException, InterruptedException {
            final KafkaCluster kafkaCluster = new KafkaCluster("");
            doReturn(kafkaCluster).when(kafkaClusterRepository).dumpCluster();

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
    class listAclBindingsForConsumer {

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void returns_a_list_with_all_acl_bindings(boolean prefix) throws ExecutionException, InterruptedException {
            final KafkaCluster kafkaCluster = new KafkaCluster("");
            doReturn(kafkaCluster).when(kafkaClusterRepository).dumpCluster();

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
    class listAclBindingsForProducer {

        @Test
        void returns_a_list_with_all_acl_bindings() throws ExecutionException, InterruptedException {
            final KafkaCluster kafkaCluster = new KafkaCluster("");
            doReturn(kafkaCluster).when(kafkaClusterRepository).dumpCluster();

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
    class listPrincipals {

        @Test
        void returns_a_set_of_principals() {
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
    class getResourceName {

        @Test
        void returns_fullname_with_dot_for_visibility() {
            final Visibility visibility = new Visibility();
            visibility.setPrefix("de.volkerfaas.arc.");
            visibility.setType(Visibility.Type.PUBLIC);

            final String resourceName = accessControlService.getResourceName(visibility, true);
            assertNotNull(resourceName);
            assertEquals("de.volkerfaas.arc.public.", resourceName);
        }

        @Test
        void returns_fullname_without_dot_for_topic() {
            final Topic topic = new Topic();
            topic.setPrefix("de.volkerfaas.arc.public.");
            topic.setName("user_updated");

            final String resourceName = accessControlService.getResourceName(topic, false);
            assertNotNull(resourceName);
            assertEquals("de.volkerfaas.arc.public.user_updated", resourceName);
        }

    }

    @Nested
    class isAclNotAvailable {

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void returns_true_in_case_acl_is_not_in_list(boolean prefix) {
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
        void returns_false_in_case_acl_is_in_list(boolean prefix) {
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
    class listNewAclBindings {

        @Test
        void test() throws ExecutionException, InterruptedException {
            final KafkaCluster kafkaCluster = new KafkaCluster("");
            doReturn(kafkaCluster).when(kafkaClusterRepository).dumpCluster();

            TopologyFile topology = new TopologyFile();

            Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            domain.setPrincipal("User:129849");
            topology.setDomain(domain);

            Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");
            visibility.getConsumers().add(new AccessControl("User:129850"));
            domain.getVisibilities().add(visibility);

            Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setNumPartitions(5);
            topic.setReplicationFactor((short) 2);
            topic.setPrefix("de.volkerfaas.arc.public.");
            topic.getConsumers().add(new AccessControl("User:129851"));
            visibility.getTopics().add(topic);

            final Set<AclBinding> newAclBindings = accessControlService.listNewAclBindings(List.of(domain));
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
    class listOrphanedAclBindings {

        @Test
        void returns_orphaned_acl_bindings_when_domain_is_removed() throws ExecutionException, InterruptedException {
            final String domainNameArc = "de.volkerfaas.arc";
            final String principalArc = "User:129849";
            final String resourceNameArc = domainNameArc + ".";
            final String domainNameTest = "de.volkerfaas.test";
            final String principalTest = "User:138166";
            final String resourceNameTest = domainNameTest + ".";

            final String domainNameRemoved = "de.volkerfaas.other";
            final String principalRemoved = "User:130777";
            final String resourceNameRemoved = domainNameRemoved + ".";

            List<Domain> domains = List.of(
                    new Domain(domainNameArc, principalArc),
                    new Domain(domainNameTest, principalTest)
            );

            final KafkaCluster kafkaCluster = new KafkaCluster("");
            doReturn(kafkaCluster).when(kafkaClusterRepository).dumpCluster();
            kafkaCluster.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(resourceNameArc, principalArc, true));
            kafkaCluster.getAclBindings().addAll(accessControlService.listAclBindingsForProducer(resourceNameArc, principalArc));
            kafkaCluster.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(resourceNameTest, principalTest, true));
            kafkaCluster.getAclBindings().addAll(accessControlService.listAclBindingsForProducer(resourceNameTest, principalTest));
            kafkaCluster.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(resourceNameRemoved, principalRemoved, true));
            kafkaCluster.getAclBindings().addAll(accessControlService.listAclBindingsForProducer(resourceNameRemoved, principalRemoved));

            final Set<AclBindingFilter> orphanedAclBindingFilters = accessControlService.listOrphanedAclBindings(domains);
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
