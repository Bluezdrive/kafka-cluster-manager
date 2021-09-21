package de.volkerfaas.kafka.topology.services.impl;

import de.volkerfaas.kafka.cluster.model.ClusterConfiguration;
import de.volkerfaas.kafka.cluster.repositories.impl.KafkaClusterRepositoryImpl;
import de.volkerfaas.kafka.topology.model.*;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collection;
import java.util.Collections;
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

            final Set<AclBindingFilter> orphanedAclBindings = accessControlService.listOrphanedAclBindingFilters(visibility, prefix, Collections.emptyList());
            assertEquals(2, orphanedAclBindings.size());
            assertThat(orphanedAclBindings, containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.DESCRIBE, prefix).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.READ, prefix).toFilter()
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

            final Set<AclBindingFilter> orphanedAclBindings = accessControlService.listOrphanedAclBindingFilters(topic, prefix, Collections.emptyList());
            assertEquals(2, orphanedAclBindings.size());
            assertThat(orphanedAclBindings, containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.DESCRIBE, prefix).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.READ, prefix).toFilter()
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

            final Set<AclBindingFilter> orphanedAclBindingFilters = accessControlService.listOrphanedAclBindingFilters(visibility, prefix, Collections.emptyList());
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
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForProducer(resourceName, principal, true));

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
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForProducer(resourceName, oldPrincipal, true));
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(otherResourceName, otherPrincipal, true));
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForProducer(otherResourceName, otherPrincipal, true));

            final Set<AclBindingFilter> orphanedAclBindingFilters = accessControlService.listOrphanedAclBindingFilters(domain);
            assertEquals(5, orphanedAclBindingFilters.size());
            assertThat(orphanedAclBindingFilters, containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, oldPrincipal, AclOperation.DESCRIBE, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, oldPrincipal, AclOperation.READ, true).toFilter(),
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
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForProducer(resourceNameArc, principalArc, true));
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(resourceNameTest, principalTest, true));
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForProducer(resourceNameTest, principalTest, true));
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(resourceNameRemoved, principalRemoved, true));
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForProducer(resourceNameRemoved, principalRemoved, true));

            final Set<AclBindingFilter> orphanedAclBindingFilters = accessControlService.listAclBindingFiltersNotInDomains(domains);
            assertEquals(5, orphanedAclBindingFilters.size());
            assertThat(orphanedAclBindingFilters, containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceNameRemoved, principalRemoved, AclOperation.DESCRIBE, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceNameRemoved, principalRemoved, AclOperation.READ, true).toFilter(),
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
        @DisplayName("should return a set of consumer AclBinding for a visibility when a principal is set as consumer")
        void testListVisibilityConsumerAclBindingsPrincipal() throws ExecutionException, InterruptedException {
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("");
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");

            final String principal = "User:129849";
            final AccessControl accessControl = new AccessControl();
            accessControl.setPrincipal(principal);
            visibility.getConsumers().add(accessControl);

            final Set<AclBinding> visibilityAclBindings = accessControlService.listConsumerAclBindingsForVisibilityOrTopic(visibility, true, Collections.emptyList());
            assertEquals(2, visibilityAclBindings.size());
            final String resourceName = visibility.getFullName() + ".";
            assertThat(visibilityAclBindings, containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.DESCRIBE, true),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.READ, true)
            ));
        }

        @Test
        @DisplayName("should return a set of producer AclBinding for a visibility when a principal is set as producer")
        void testListVisibilityProducerAclBindingsPrincipal() throws ExecutionException, InterruptedException {
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("");
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");

            final String principal = "User:129849";
            final AccessControl accessControl = new AccessControl();
            accessControl.setPrincipal(principal);
            visibility.getProducers().add(accessControl);

            final Set<AclBinding> visibilityAclBindings = accessControlService.listProducerAclBindingsForVisibilityOrTopic(visibility, true, Collections.emptyList());
            assertEquals(3, visibilityAclBindings.size());
            final String resourceName = visibility.getFullName() + ".";
            assertThat(visibilityAclBindings, containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.WRITE, true),
                    accessControlService.getAclBinding(ResourceType.TRANSACTIONAL_ID, resourceName, principal, AclOperation.WRITE, true),
                    accessControlService.getAclBinding(ResourceType.CLUSTER, "kafka-cluster", principal, AclOperation.IDEMPOTENT_WRITE, false)
            ));
        }

        @Test
        @DisplayName("should return a set of consumer AclBinding for a visibility when a domain name is set as consumer")
        void testListVisibilityConsumerAclBindingsDomainName() throws ExecutionException, InterruptedException {
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("");
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

            final String principal = "User:129849";
            final Domain domainArc = new Domain("de.volkerfaas.arc", "User:123456");
            final Domain domainTest = new Domain("de.volkerfaas.test", principal);
            final Collection<Domain> domains = List.of(domainArc, domainTest);

            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");

            final AccessControl accessControl = new AccessControl();
            accessControl.setDomain("de.volkerfaas.test");
            visibility.getConsumers().add(accessControl);

            final Set<AclBinding> visibilityAclBindings = accessControlService.listConsumerAclBindingsForVisibilityOrTopic(visibility, true, domains);
            assertEquals(2, visibilityAclBindings.size());
            final String resourceName = visibility.getFullName() + ".";
            assertThat(visibilityAclBindings, containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.DESCRIBE, true),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.READ, true)
            ));
        }

        @Test
        @DisplayName("should return a set of producer AclBinding for a visibility when a domain name is set as consumer")
        void testListVisibilityProducerAclBindingsDomainName() throws ExecutionException, InterruptedException {
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("");
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

            final String principal = "User:129849";
            final Domain domainArc = new Domain("de.volkerfaas.arc", "User:123456");
            final Domain domainTest = new Domain("de.volkerfaas.test", principal);
            final Collection<Domain> domains = List.of(domainArc, domainTest);

            final Visibility visibility = new Visibility();
            visibility.setType(Visibility.Type.PUBLIC);
            visibility.setPrefix("de.volkerfaas.arc.");

            final AccessControl accessControl = new AccessControl();
            accessControl.setDomain("de.volkerfaas.test");
            visibility.getProducers().add(accessControl);

            final Set<AclBinding> visibilityAclBindings = accessControlService.listProducerAclBindingsForVisibilityOrTopic(visibility, true, domains);
            assertEquals(3, visibilityAclBindings.size());
            final String resourceName = visibility.getFullName() + ".";
            assertThat(visibilityAclBindings, containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.WRITE, true),
                    accessControlService.getAclBinding(ResourceType.TRANSACTIONAL_ID, resourceName, principal, AclOperation.WRITE, true),
                    accessControlService.getAclBinding(ResourceType.CLUSTER, "kafka-cluster", principal, AclOperation.IDEMPOTENT_WRITE, false)
            ));
        }

        @Test
        @DisplayName("should return a set of consumer AclBinding for a topic when a principal is set as consumer")
        void testListTopicConsumerAclBindingsPrincipal() throws ExecutionException, InterruptedException {
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("");
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

            final Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setPrefix("de.volkerfaas.arc.public.");

            final String principal = "User:129849";
            final AccessControl accessControl = new AccessControl();
            accessControl.setPrincipal(principal);
            topic.getConsumers().add(accessControl);

            final Set<AclBinding> topicAclBindings = accessControlService.listConsumerAclBindingsForVisibilityOrTopic(topic, false, Collections.emptyList());
            assertEquals(2, topicAclBindings.size());
            final String resourceName = topic.getFullName();
            assertThat(topicAclBindings, containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.DESCRIBE, false),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.READ, false)
            ));
        }

        @Test
        @DisplayName("should return a set of producer AclBinding for a topic when a principal is set as consumer")
        void testListTopicProducerAclBindingsPrincipal() throws ExecutionException, InterruptedException {
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("");
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

            final Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setPrefix("de.volkerfaas.arc.public.");

            final String principal = "User:129849";
            final AccessControl accessControl = new AccessControl();
            accessControl.setPrincipal(principal);
            topic.getProducers().add(accessControl);

            final Set<AclBinding> topicAclBindings = accessControlService.listProducerAclBindingsForVisibilityOrTopic(topic, false, Collections.emptyList());
            assertEquals(3, topicAclBindings.size());
            final String resourceName = topic.getFullName();
            assertThat(topicAclBindings, containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.WRITE, false),
                    accessControlService.getAclBinding(ResourceType.TRANSACTIONAL_ID, resourceName, principal, AclOperation.WRITE, false),
                    accessControlService.getAclBinding(ResourceType.CLUSTER, "kafka-cluster", principal, AclOperation.IDEMPOTENT_WRITE, false)
            ));
        }

        @Test
        @DisplayName("should return a set of consumer AclBinding for a topic when a domain name is set as consumer")
        void testListTopicConsumerAclBindingsDomainName() throws ExecutionException, InterruptedException {
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("");
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

            final String principal = "User:129849";
            final Domain domainArc = new Domain("de.volkerfaas.arc", "User:123456");
            final Domain domainTest = new Domain("de.volkerfaas.test", principal);
            final Collection<Domain> domains = List.of(domainArc, domainTest);

            final Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setPrefix("de.volkerfaas.arc.public.");

            final AccessControl accessControl = new AccessControl();
            accessControl.setDomain("de.volkerfaas.test");
            topic.getConsumers().add(accessControl);

            final Set<AclBinding> topicAclBindings = accessControlService.listConsumerAclBindingsForVisibilityOrTopic(topic, false, domains);
            assertEquals(2, topicAclBindings.size());
            final String resourceName = topic.getFullName();
            assertThat(topicAclBindings, containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.DESCRIBE, false),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.READ, false)
            ));
        }

        @Test
        @DisplayName("should return a set of producer AclBinding for a topic when a domain name is set as consumer")
        void testListTopicProducerAclBindingsDomainName() throws ExecutionException, InterruptedException {
            final ClusterConfiguration clusterConfiguration = new ClusterConfiguration("");
            doReturn(clusterConfiguration).when(kafkaClusterRepository).getClusterConfiguration();

            final String principal = "User:129849";
            final Domain domainArc = new Domain("de.volkerfaas.arc", "User:123456");
            final Domain domainTest = new Domain("de.volkerfaas.test", principal);
            final Collection<Domain> domains = List.of(domainArc, domainTest);

            final Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setPrefix("de.volkerfaas.arc.public.");

            final AccessControl accessControl = new AccessControl();
            accessControl.setDomain("de.volkerfaas.test");
            topic.getProducers().add(accessControl);

            final Set<AclBinding> topicAclBindings = accessControlService.listProducerAclBindingsForVisibilityOrTopic(topic, false, domains);
            assertEquals(3, topicAclBindings.size());
            final String resourceName = topic.getFullName();
            assertThat(topicAclBindings, containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.WRITE, false),
                    accessControlService.getAclBinding(ResourceType.TRANSACTIONAL_ID, resourceName, principal, AclOperation.WRITE, false),
                    accessControlService.getAclBinding(ResourceType.CLUSTER, "kafka-cluster", principal, AclOperation.IDEMPOTENT_WRITE, false)
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
            assertEquals(2, consumerAclBindings.size());
            assertThat(consumerAclBindings, containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.DESCRIBE, prefix),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.READ, prefix)
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

            final Set<AclBinding> producerAclBindings = accessControlService.listAclBindingsForProducer(resourceName, principal, true);
            assertEquals(3, producerAclBindings.size());
            assertThat(producerAclBindings, containsInAnyOrder(
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

            final Set<String> consumerPrincipals = accessControlService.listPrincipals(visibility, Collections.emptyList());
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
            assertEquals(10, newAclBindings.size());
            assertThat(newAclBindings, containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, "de.volkerfaas.arc.", "User:129849", AclOperation.WRITE, true),
                    accessControlService.getAclBinding(ResourceType.TRANSACTIONAL_ID, "de.volkerfaas.arc.", "User:129849", AclOperation.WRITE, true),
                    accessControlService.getAclBinding(ResourceType.CLUSTER, "kafka-cluster", "User:129849", AclOperation.IDEMPOTENT_WRITE, false),
                    accessControlService.getAclBinding(ResourceType.TOPIC, "de.volkerfaas.arc.", "User:129849", AclOperation.DESCRIBE, true),
                    accessControlService.getAclBinding(ResourceType.TOPIC, "de.volkerfaas.arc.", "User:129849", AclOperation.READ, true),
                    accessControlService.getAclBinding(ResourceType.GROUP, "de.volkerfaas.arc.", "User:129849", AclOperation.READ, true),
                    accessControlService.getAclBinding(ResourceType.TOPIC, "de.volkerfaas.arc.public.", "User:129850", AclOperation.DESCRIBE, true),
                    accessControlService.getAclBinding(ResourceType.TOPIC, "de.volkerfaas.arc.public.", "User:129850", AclOperation.READ, true),
                    accessControlService.getAclBinding(ResourceType.TOPIC, "de.volkerfaas.arc.public.user_updated", "User:129851", AclOperation.DESCRIBE, false),
                    accessControlService.getAclBinding(ResourceType.TOPIC, "de.volkerfaas.arc.public.user_updated", "User:129851", AclOperation.READ, false)
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
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForProducer(resourceNameArc, principalArc, true));
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(resourceNameTest, principalTest, true));
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForProducer(resourceNameTest, principalTest, true));
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForConsumer(resourceNameRemoved, principalRemoved, true));
            clusterConfiguration.getAclBindings().addAll(accessControlService.listAclBindingsForProducer(resourceNameRemoved, principalRemoved, true));

            final Collection<AclBindingFilter> orphanedAclBindingFilters = accessControlService.listOrphanedAclBindings(domains);
            assertNotNull(orphanedAclBindingFilters);
            assertThat(orphanedAclBindingFilters, containsInAnyOrder(
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceNameRemoved, principalRemoved, AclOperation.DESCRIBE, true).toFilter(),
                    accessControlService.getAclBinding(ResourceType.TOPIC, resourceNameRemoved, principalRemoved, AclOperation.READ, true).toFilter(),
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
    @DisplayName("the method findDomainPrincipalByDomainName")
    class FindDomainPrincipalByDomainName {

        @Test
        @DisplayName("should return the principal in case the domain exists and has a principal")
        void testFindDomainPrincipalByDomainName() {
            final Domain domainArc = new Domain("de.volkerfaas.arc", "User:987654");
            final Domain domainTest = new Domain("de.volkerfaas.test", "User:123456");
            final Collection<Domain> domains = List.of(domainArc, domainTest);
            final String principal = accessControlService.findDomainPrincipalByDomainName(domains, "de.volkerfaas.arc");
            assertEquals("User:987654", principal);
        }

        @Test
        @DisplayName("should return null in case the domain doesn't exist")
        void testFindDomainPrincipalByDomainNameNotFound() {
            final Domain domainTest = new Domain("de.volkerfaas.test", "User:123456");
            final Collection<Domain> domains = List.of(domainTest);
            final String principal = accessControlService.findDomainPrincipalByDomainName(domains, "de.volkerfaas.arc");
            assertNull(principal);
        }

        @Test
        @DisplayName("should return null in case the domain exists, but doesn't have a principal")
        void testFindDomainPrincipalByDomainNameNoPrincipal() {
            final Domain domainArc = new Domain("de.volkerfaas.arc");
            final Domain domainTest = new Domain("de.volkerfaas.test", "User:123456");
            final Collection<Domain> domains = List.of(domainArc, domainTest);
            final String principal = accessControlService.findDomainPrincipalByDomainName(domains, "de.volkerfaas.arc");
            assertNull(principal);
        }

    }

    @Nested
    @DisplayName("the method findPrincipalByResourceName")
    class FindPrincipalByResourceName {

        @Test
        void testFindPrincipalByResourceName() {
            final String resourceName = "de.volkerfaas.test.public.user_updated";
            final ResourcePattern resourcePattern = new ResourcePattern(ResourceType.TOPIC, resourceName, PatternType.LITERAL);
            final AccessControlEntry accessControlEntry = new AccessControlEntry("User:123456", "*", AclOperation.READ, AclPermissionType.ALLOW);
            final AclBinding aclBinding = new AclBinding(resourcePattern, accessControlEntry);
            final Collection<AclBinding> aclBindings = List.of(aclBinding);
            final String principal = accessControlService.findPrincipalByResourceName(aclBindings, resourceName);
            assertEquals("User:123456", principal);
        }

        @Test
        void testFindPrincipalByResourceNameNotFound() {
            final String resourceName = "de.volkerfaas.test.public.user_updated";
            final ResourcePattern resourcePattern = new ResourcePattern(ResourceType.TOPIC, resourceName, PatternType.LITERAL);
            final AccessControlEntry accessControlEntry = new AccessControlEntry("User:123456", "*", AclOperation.READ, AclPermissionType.ALLOW);
            final AclBinding aclBinding = new AclBinding(resourcePattern, accessControlEntry);
            final Collection<AclBinding> aclBindings = List.of(aclBinding);
            final String principal = accessControlService.findPrincipalByResourceName(aclBindings, "de.volkerfaas.test.public.card_created");
            assertNull(principal);
        }

    }

    @Nested
    @DisplayName("the method findPrincipalsByResourceName")
    class FindPrincipalsByResourceName {

        @Test
        void testFindPrincipalsByResourceName() {
            final String resourceName = "de.volkerfaas.test.public.user_updated";
            final ResourcePattern resourcePattern = new ResourcePattern(ResourceType.TOPIC, resourceName, PatternType.LITERAL);
            final AccessControlEntry accessControlEntry = new AccessControlEntry("User:123456", "*", AclOperation.READ, AclPermissionType.ALLOW);
            final AclBinding aclBinding = new AclBinding(resourcePattern, accessControlEntry);
            final Collection<AclBinding> aclBindings = List.of(aclBinding);
            final Collection<String> principals = accessControlService.findPrincipalsByResourceName(aclBindings, resourceName);
            assertThat(principals, containsInAnyOrder("User:123456"));
        }

        @Test
        void testFindPrincipalsByResourceNameNotFound() {
            final String resourceName = "de.volkerfaas.test.public.user_updated";
            final ResourcePattern resourcePattern = new ResourcePattern(ResourceType.TOPIC, resourceName, PatternType.LITERAL);
            final AccessControlEntry accessControlEntry = new AccessControlEntry("User:123456", "*", AclOperation.READ, AclPermissionType.ALLOW);
            final AclBinding aclBinding = new AclBinding(resourcePattern, accessControlEntry);
            final Collection<AclBinding> aclBindings = List.of(aclBinding);
            final Collection<String> principals = accessControlService.findPrincipalsByResourceName(aclBindings, "de.volkerfaas.test.public.card_created");
            assertThat(principals, not(containsInAnyOrder("User:123456")));
        }

    }

}
