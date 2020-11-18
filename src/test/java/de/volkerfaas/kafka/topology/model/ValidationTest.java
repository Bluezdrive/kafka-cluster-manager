package de.volkerfaas.kafka.topology.model;

import de.volkerfaas.kafka.cluster.model.PartitionConfiguration;
import de.volkerfaas.kafka.cluster.model.TopicConfiguration;
import de.volkerfaas.kafka.topology.ApplicationConfiguration;
import de.volkerfaas.kafka.topology.validation.ValidatorPayload;
import org.hibernate.validator.HibernateValidator;
import org.hibernate.validator.HibernateValidatorFactory;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@DisplayName("The validation of")
class ValidationTest {

    private HibernateValidatorFactory validatorFactory;
    private String directory;

    @BeforeAll
    static void setup() {
        Locale.setDefault(Locale.ENGLISH);
    }

    @BeforeEach
    void init() {
        final URL resource = getClass().getClassLoader().getResource("topology-de.volkerfaas.arc.yaml");
        assertNotNull(resource);
        this.directory = new File(resource.getPath()).getParent();
        System.setProperty(ApplicationConfiguration.PROPERTY_KEY_TOPOLOGY_DIRECTORY, this.directory);

        this.validatorFactory = Validation.byProvider(HibernateValidator.class)
                .configure()
                .buildValidatorFactory()
                .unwrap(HibernateValidatorFactory.class);
    }

    @Nested
    @DisplayName("of a topology file")
    class TopologyFileValidation {

        @Test
        @DisplayName("should pass if the topology file matches the minimum requirements")
        void testTopologyFileValidation() {
            final TopologyFile topology = new TopologyFile();
            topology.setFile(new File(directory, "topology-de.volkerfaas.arc.yaml"));

            final Team maintainer = new Team();
            maintainer.setName("Volker Faas");
            maintainer.setEmail("bluezdrive@volkerfaas.de");

            final Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            domain.setPrincipal("User:129849");
            domain.setDescription("Domain for testing architecture stuff with Apache Kafka® cluster.");
            domain.setMaintainer(maintainer);
            topology.setDomain(domain);

            final Validator validator = validatorFactory.getValidator();
            final Set<ConstraintViolation<TopologyFile>> violations = validator.validate(topology);
            assertNotNull(violations);
            assertEquals(0, violations.size());
        }

        @Test
        @DisplayName("should fail if the topology file has no file set")
        void testTopologyFileValidationNoFile() {
            final TopologyFile topology = new TopologyFile();

            final Validator validator = validatorFactory.getValidator();
            final Set<ConstraintViolation<TopologyFile>> violations = validator.validate(topology);
            assertNotNull(violations);
            assertEquals(1, violations.size());

            final ConstraintViolation<TopologyFile> violation = violations.stream().findFirst().orElse(null);
            assertNotNull(violation);
            assertEquals("must not be null", violation.getMessage());
        }

        @Test
        @DisplayName("should fail if the topology file has no domain in filename")
        void testTopologyFileValidationNoDomainInFile() {
            final TopologyFile topology = new TopologyFile();
            topology.setFile(new File(directory, "topology-de.volkerfaas.foo.yaml"));

            final Team maintainer = new Team();
            maintainer.setName("Volker Faas");
            maintainer.setEmail("bluezdrive@volkerfaas.de");

            final Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            domain.setPrincipal("User:129849");
            domain.setDescription("Domain for testing architecture stuff with Apache Kafka® cluster.");
            domain.setMaintainer(maintainer);
            topology.setDomain(domain);

            final Validator validator = validatorFactory.getValidator();
            final Set<ConstraintViolation<TopologyFile>> violations = validator.validate(topology);
            assertNotNull(violations);
            assertEquals(1, violations.size());

            final ConstraintViolation<TopologyFile> violation = violations.stream().findFirst().orElse(null);
            assertNotNull(violation);
            assertEquals("must have domain name de.volkerfaas.arc in file name", violation.getMessage());
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "foo-de.volkerfaas.arc.yaml",
                "topology-1.volkerfaas.arc.yaml"
        })
        @DisplayName("should fail if the topology file has an invalid domain in filename")
        void testTopologyFileValidationInvalidDomainInFile(String filename) {
            final TopologyFile topology = new TopologyFile();
            topology.setFile(new File(directory, filename));

            final Validator validator = validatorFactory.getValidator();
            final Set<ConstraintViolation<TopologyFile>> violations = validator.validate(topology);
            assertNotNull(violations);
            assertEquals(1, violations.size());

            final ConstraintViolation<TopologyFile> violation = violations.stream().findFirst().orElse(null);
            assertNotNull(violation);
            assertEquals("must match topology\\-([a-z]+\\.[a-z]+\\.[_a-z]+)\\.yaml", violation.getMessage());
        }


    }

    @Nested
    @DisplayName("of a domain")
    class DomainValidation {

        @Test
        @DisplayName("should pass if the domain matches the minimum requirements")
        void testDomainValidation() {
            final Team maintainer = new Team();
            maintainer.setName("Volker Faas");
            maintainer.setEmail("bluezdrive@volkerfaas.de");

            final Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            domain.setDescription("Domain for testing architecture stuff with Apache Kafka® cluster.");
            domain.setPrincipal("User:129849");
            domain.setMaintainer(maintainer);

            final Visibility publicVisibility = new Visibility();
            publicVisibility.setType(Visibility.Type.PUBLIC);
            domain.getVisibilities().add(publicVisibility);

            final Visibility protectedVisibility = new Visibility();
            protectedVisibility.setType(Visibility.Type.PROTECTED);
            domain.getVisibilities().add(protectedVisibility);

            final Visibility privateVisibility = new Visibility();
            privateVisibility.setType(Visibility.Type.PRIVATE);
            domain.getVisibilities().add(privateVisibility);

            final Validator validator = validatorFactory.getValidator();
            final Set<ConstraintViolation<Domain>> violations = validator.validate(domain);
            assertEquals(0, violations.size());
        }

        @Test
        @DisplayName("should fail if the domain has no name set")
        void testDomainValidationNameNull() {
            final Team maintainer = new Team();
            maintainer.setName("Volker Faas");
            maintainer.setEmail("bluezdrive@volkerfaas.de");

            final Domain domain = new Domain();
            domain.setName(null);
            domain.setPrincipal("User:129849");
            domain.setDescription("Domain for testing architecture stuff with Apache Kafka® cluster.");
            domain.setMaintainer(maintainer);

            final Validator validator = validatorFactory.getValidator();
            final Set<ConstraintViolation<Domain>> violations = validator.validate(domain);
            assertEquals(1, violations.size());

            final ConstraintViolation<Domain> violation = violations.stream().findFirst().orElse(null);
            assertNotNull(violation);
            assertEquals("must not be null", violation.getMessage());
        }

        @ParameterizedTest
        @NullAndEmptySource
        @ValueSource(strings = {" "})
        @DisplayName("should fail if the domain has an invalid description set")
        void testDomainValidationDescriptionInvalid(String description) {
            final Team maintainer = new Team();
            maintainer.setName("Volker Faas");
            maintainer.setEmail("bluezdrive@volkerfaas.de");

            final Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            domain.setPrincipal("User:129849");
            domain.setDescription(description);
            domain.setMaintainer(maintainer);

            final Validator validator = validatorFactory.getValidator();
            final Set<ConstraintViolation<Domain>> violations = validator.validate(domain);
            assertEquals(1, violations.size());

            final ConstraintViolation<Domain> violation = violations.stream().findFirst().orElse(null);
            assertNotNull(violation);
            assertEquals("must not be blank", violation.getMessage());
        }

        @ParameterizedTest
        @ValueSource(strings = {" "})
        @DisplayName("should fail if the domain has an invalid name set")
        void testDomainValidationNameInvalid(String name) {
            final Team maintainer = new Team();
            maintainer.setName("Volker Faas");
            maintainer.setEmail("bluezdrive@volkerfaas.de");

            final Domain domain = new Domain();
            domain.setName(name);
            domain.setPrincipal("User:129849");
            domain.setDescription("Domain for testing architecture stuff with Apache Kafka® cluster.");
            domain.setMaintainer(maintainer);

            final Validator validator = validatorFactory.getValidator();
            final Set<ConstraintViolation<Domain>> violations = validator.validate(domain);
            assertEquals(1, violations.size());

            final ConstraintViolation<Domain> violation = violations.stream().findFirst().orElse(null);
            assertNotNull(violation);
            assertEquals("must match \"^([a-z]+)\\.([a-z]+)\\.([a-z]+)$\"", violation.getMessage());
        }

        @Test
        @DisplayName("should fail if the domain has no principal set")
        void testDomainValidationPrincipalNull() {
            final Team maintainer = new Team();
            maintainer.setName("Volker Faas");
            maintainer.setEmail("bluezdrive@volkerfaas.de");

            final Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            domain.setDescription("Domain for testing architecture stuff with Apache Kafka® cluster.");
            domain.setPrincipal(null);
            domain.setMaintainer(maintainer);

            final Validator validator = validatorFactory.getValidator();
            final Set<ConstraintViolation<Domain>> violations = validator.validate(domain);
            assertEquals(1, violations.size());

            final ConstraintViolation<Domain> violation = violations.stream().findFirst().orElse(null);
            assertNotNull(violation);
            assertEquals("must not be null", violation.getMessage());
        }

        @ParameterizedTest
        @EmptySource
        @ValueSource(strings = {
                " ",
                "X:129849",
                "User:A"
        })
        @DisplayName("should fail if the domain has an invalid principal set")
        void testDomainValidationPrincipalInvalid(String principal) {
            final Team maintainer = new Team();
            maintainer.setName("Volker Faas");
            maintainer.setEmail("bluezdrive@volkerfaas.de");

            final Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            domain.setPrincipal(principal);
            domain.setDescription("Domain for testing architecture stuff with Apache Kafka® cluster.");
            domain.setMaintainer(maintainer);

            final Validator validator = validatorFactory.getValidator();
            final Set<ConstraintViolation<Domain>> violations = validator.validate(domain);
            assertEquals(1, violations.size());

            final ConstraintViolation<Domain> violation = violations.stream().findFirst().orElse(null);
            assertNotNull(violation);
            assertEquals("must match \"^(User)+\\:([0-9]+)*$\"", violation.getMessage());
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "public",
                "protected",
                "private"
        })
        @DisplayName("should fail if the domain has an more than one visibility of each type")
        void testDomainValidationVisibilityDuplicates(String type) {
            final Team maintainer = new Team();
            maintainer.setName("Volker Faas");
            maintainer.setEmail("bluezdrive@volkerfaas.de");

            final Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            domain.setPrincipal("User:129849");
            domain.setDescription("Domain for testing architecture stuff with Apache Kafka® cluster.");
            domain.setMaintainer(maintainer);

            final Visibility publicVisibility = new Visibility();
            publicVisibility.setType(Visibility.Type.valueOf(type.toUpperCase()));
            domain.getVisibilities().add(publicVisibility);
            domain.getVisibilities().add(publicVisibility);
            domain.getVisibilities().add(publicVisibility);

            final Validator validator = validatorFactory.getValidator();
            final Set<ConstraintViolation<Domain>> violations = validator.validate(domain);
            assertNotNull(violations);
            assertEquals(1, violations.size());

            final ConstraintViolation<Domain> violation = violations.stream().findFirst().orElse(null);
            assertNotNull(violation);
            assertEquals("must only contain zero or one element of type '" + type + "'", violation.getMessage());
        }

    }

    @Nested
    @DisplayName("of a visibility")
    class VisibilityValidation {

        @Test
        @DisplayName("should pass if the consumer principal is set in a domain")
        void testVisibilityValidationConsumerPrincipalIsInDomain() {
            final TopologyFile topologyArc = new TopologyFile();
            topologyArc.setFile(new File(directory, "topology-de.volkerfaas.arc.yaml"));

            final Team maintainer = new Team();
            maintainer.setName("Volker Faas");
            maintainer.setEmail("bluezdrive@volkerfaas.de");

            final Domain domainArc = new Domain();
            domainArc.setName("de.volkerfaas.arc");
            domainArc.setDescription("Domain for testing architecture stuff with Apache Kafka® cluster.");
            domainArc.setPrincipal("User:129849");
            domainArc.setMaintainer(maintainer);
            topologyArc.setDomain(domainArc);

            AccessControl consumer = new AccessControl();
            consumer.setPrincipal("User:138166");

            final Visibility publicVisibilityArc = new Visibility();
            publicVisibilityArc.setType(Visibility.Type.PUBLIC);
            publicVisibilityArc.getConsumers().add(consumer);
            domainArc.getVisibilities().add(publicVisibilityArc);

            final TopologyFile topologyTest = new TopologyFile();
            topologyTest.setFile(new File(directory, "topology-de.volkerfaas.test.yaml"));

            final Domain domainTest = new Domain();
            domainTest.setName("de.volkerfaas.test");
            domainTest.setDescription("Domain for testing architecture stuff with Apache Kafka® cluster.");
            domainTest.setPrincipal("User:138166");
            domainTest.setMaintainer(maintainer);
            topologyTest.setDomain(domainTest);

            final Visibility publicVisibilityTest = new Visibility();
            publicVisibilityTest.setType(Visibility.Type.PUBLIC);
            domainTest.getVisibilities().add(publicVisibilityTest);

            final Set<TopologyFile> topologies = Set.of(topologyArc, topologyTest);
            final ValidatorPayload validatorPayload = new ValidatorPayload(topologies, Collections.emptyList());
            final Validator validator = validatorFactory.usingContext().constraintValidatorPayload(validatorPayload).getValidator();
            final Set<ConstraintViolation<TopologyFile>> violations = topologies.stream()
                    .map((Function<TopologyFile, Set<ConstraintViolation<TopologyFile>>>) validator::validate)
                    .flatMap(Set::stream)
                    .collect(Collectors.toUnmodifiableSet());
            assertNotNull(violations);
            assertEquals(0, violations.size());
        }

        @Test
        @DisplayName("should fail if the consumer principal doesn't exist in any domain")
        void testVisibilityValidationConsumerPrincipalIsNotInDomain() {
            final TopologyFile topologyArc = new TopologyFile();
            topologyArc.setFile(new File(directory, "topology-de.volkerfaas.arc.yaml"));

            final Team maintainer = new Team();
            maintainer.setName("Volker Faas");
            maintainer.setEmail("bluezdrive@volkerfaas.de");

            final Domain domainArc = new Domain();
            domainArc.setName("de.volkerfaas.arc");
            domainArc.setDescription("Domain for testing architecture stuff with Apache Kafka® cluster.");
            domainArc.setPrincipal("User:129849");
            domainArc.setMaintainer(maintainer);
            topologyArc.setDomain(domainArc);

            AccessControl consumer = new AccessControl();
            consumer.setPrincipal("User:134652");

            final Visibility publicVisibilityArc = new Visibility();
            publicVisibilityArc.setType(Visibility.Type.PUBLIC);
            publicVisibilityArc.getConsumers().add(consumer);
            domainArc.getVisibilities().add(publicVisibilityArc);

            final TopologyFile topologyTest = new TopologyFile();
            topologyTest.setFile(new File(directory, "topology-de.volkerfaas.test.yaml"));

            final Domain domainTest = new Domain();
            domainTest.setName("de.volkerfaas.test");
            domainTest.setDescription("Domain for testing architecture stuff with Apache Kafka® cluster.");
            domainTest.setPrincipal("User:138166");
            domainTest.setMaintainer(maintainer);
            topologyTest.setDomain(domainTest);

            final Visibility publicVisibilityTest = new Visibility();
            publicVisibilityTest.setType(Visibility.Type.PUBLIC);
            domainTest.getVisibilities().add(publicVisibilityTest);

            final Set<TopologyFile> topologies = Set.of(topologyArc, topologyTest);
            final ValidatorPayload validatorPayload = new ValidatorPayload(topologies, Collections.emptyList());
            final Validator validator = validatorFactory.usingContext().constraintValidatorPayload(validatorPayload).getValidator();
            final Set<ConstraintViolation<TopologyFile>> violations = topologies.stream()
                    .map((Function<TopologyFile, Set<ConstraintViolation<TopologyFile>>>) validator::validate)
                    .flatMap(Set::stream)
                    .collect(Collectors.toUnmodifiableSet());
            assertNotNull(violations);
            assertEquals(1, violations.size());

            final ConstraintViolation<TopologyFile> violation = violations.stream().findFirst().orElse(null);
            assertNotNull(violation);
            assertEquals("'User:134652' doesn't exist in any domain", violation.getMessage());
        }

        @Test
        @DisplayName("should fail if the consumer principal exists, but the corresponding domain principal is not set")
        void testVisibilityValidationConsumerPrincipalIsNotInDomainAndNull() {
            final TopologyFile topologyArc = new TopologyFile();
            topologyArc.setFile(new File(directory, "topology-de.volkerfaas.arc.yaml"));

            final Team maintainer = new Team();
            maintainer.setName("Volker Faas");
            maintainer.setEmail("bluezdrive@volkerfaas.de");

            final Domain domainArc = new Domain();
            domainArc.setName("de.volkerfaas.arc");
            domainArc.setDescription("Domain for testing architecture stuff with Apache Kafka® cluster.");
            domainArc.setPrincipal("User:129849");
            domainArc.setMaintainer(maintainer);
            topologyArc.setDomain(domainArc);

            AccessControl consumer = new AccessControl();
            consumer.setPrincipal("User:138166");

            final Visibility publicVisibilityArc = new Visibility();
            publicVisibilityArc.setType(Visibility.Type.PUBLIC);
            publicVisibilityArc.getConsumers().add(consumer);
            domainArc.getVisibilities().add(publicVisibilityArc);

            final TopologyFile topologyTest = new TopologyFile();
            topologyTest.setFile(new File(directory, "topology-de.volkerfaas.test.yaml"));

            final Domain domainTest = new Domain();
            domainTest.setName("de.volkerfaas.test");
            domainTest.setDescription("Domain for testing architecture stuff with Apache Kafka® cluster.");
            domainTest.setMaintainer(maintainer);
            topologyTest.setDomain(domainTest);

            final Visibility publicVisibilityTest = new Visibility();
            publicVisibilityTest.setType(Visibility.Type.PUBLIC);
            domainTest.getVisibilities().add(publicVisibilityTest);

            final Set<TopologyFile> topologies = Set.of(topologyArc, topologyTest);
            final ValidatorPayload validatorPayload = new ValidatorPayload(topologies, Collections.emptyList());
            final Validator validator = validatorFactory.usingContext().constraintValidatorPayload(validatorPayload).getValidator();
            final Set<ConstraintViolation<TopologyFile>> violations = topologies.stream()
                    .map((Function<TopologyFile, Set<ConstraintViolation<TopologyFile>>>) validator::validate)
                    .flatMap(Set::stream)
                    .collect(Collectors.toUnmodifiableSet());
            assertNotNull(violations);
            assertEquals(2, violations.size());

            final Set<String> messages = violations.stream().map(ConstraintViolation::getMessage).collect(Collectors.toSet());
            assertNotNull(messages);
            assertEquals(2, messages.size());
            assertThat(messages, containsInAnyOrder(
                    "'User:138166' doesn't exist in any domain",
                    "must not be null"));
        }

    }

    @Nested
    @DisplayName("of a topic")
    class TopicValidation {

        @Test
        @DisplayName("should pass if the topic matches the minimum requirements")
        void testTopicValidation() {
            final ValidatorPayload validatorPayload = new ValidatorPayload(Collections.emptySet(), Collections.emptyList());
            final Validator validator = validatorFactory.usingContext().constraintValidatorPayload(validatorPayload).getValidator();

            final Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setDescription("This is the topic for the UserUpdated event.");
            topic.setValueSchemaFile("events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.avsc");
            assertEquals(6, topic.getNumPartitions());
            assertEquals(3, topic.getReplicationFactor());

            final Set<ConstraintViolation<Topic>> violations = validator.validate(topic);
            assertNotNull(violations);
            assertEquals(0, violations.size());
        }

        @Test
        @DisplayName("should fail if the topic has less than one partition")
        void testTopicValidationPartitionLessThanOne() {
            final ValidatorPayload validatorPayload = new ValidatorPayload(Collections.emptySet(), Collections.emptyList());
            final Validator validator = validatorFactory.usingContext().constraintValidatorPayload(validatorPayload).getValidator();

            final Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setDescription("This is the topic for the UserUpdated event.");
            topic.setValueSchemaFile("events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.avsc");
            topic.setNumPartitions(0);

            final Set<ConstraintViolation<Topic>> violations = validator.validate(topic);
            assertNotNull(violations);
            assertEquals(1, violations.size());

            final ConstraintViolation<Topic> violation = violations.stream().findFirst().orElse(null);
            assertNotNull(violation);
            assertEquals("must be greater than or equal to 1", violation.getMessage());
        }

        @Test
        @DisplayName("should fail if the topic has more than twenty partitions")
        void testTopicValidationPartitionGreaterThanTwenty() {
            final ValidatorPayload validatorPayload = new ValidatorPayload(Collections.emptySet(), Collections.emptyList());
            final Validator validator = validatorFactory.usingContext().constraintValidatorPayload(validatorPayload).getValidator();

            final Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setDescription("This is the topic for the UserUpdated event.");
            topic.setValueSchemaFile("events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.avsc");
            topic.setNumPartitions(21);

            final Set<ConstraintViolation<Topic>> violations = validator.validate(topic);
            assertNotNull(violations);
            assertEquals(1, violations.size());

            final ConstraintViolation<Topic> violation = violations.stream().findFirst().orElse(null);
            assertNotNull(violation);
            assertEquals("must be less than or equal to 20", violation.getMessage());
        }

        @Test
        @DisplayName("should pass if the topic has equal partitions than in cluster")
        void testTopicValidationPartitionsEqual() {
            final String fullTopicName = "de.volkerfaas.arc.public.user_updated";
            final List<PartitionConfiguration> partitions = List.of(new PartitionConfiguration(fullTopicName, 0), new PartitionConfiguration(fullTopicName, 1), new PartitionConfiguration(fullTopicName, 2), new PartitionConfiguration(fullTopicName, 3));
            final List<TopicConfiguration> topicConfigurations = List.of(new TopicConfiguration(fullTopicName, partitions, (short) 3, Collections.emptyMap()));
            final ValidatorPayload validatorPayload = new ValidatorPayload(Collections.emptySet(), topicConfigurations);
            final Validator validator = validatorFactory.usingContext().constraintValidatorPayload(validatorPayload).getValidator();

            final Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setPrefix("de.volkerfaas.arc.public.");
            topic.setDescription("This is the topic for the UserUpdated event.");
            topic.setValueSchemaFile("events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.avsc");
            topic.setNumPartitions(4);

            final Set<ConstraintViolation<Topic>> violations = validator.validate(topic);
            assertNotNull(violations);
            assertEquals(0, violations.size());
        }

        @Test
        @DisplayName("should fail if the topic has less partitions than in cluster")
        void testTopicValidationPartitionsLess() {
            final String fullTopicName = "de.volkerfaas.arc.public.user_updated";
            final List<PartitionConfiguration> partitions = List.of(new PartitionConfiguration(fullTopicName, 0), new PartitionConfiguration(fullTopicName, 1), new PartitionConfiguration(fullTopicName, 2), new PartitionConfiguration(fullTopicName, 3));
            final List<TopicConfiguration> topicConfigurations = List.of(new TopicConfiguration(fullTopicName, partitions, (short) 3, Collections.emptyMap()));
            final ValidatorPayload validatorPayload = new ValidatorPayload(Collections.emptySet(), topicConfigurations);
            final Validator validator = validatorFactory.usingContext().constraintValidatorPayload(validatorPayload).getValidator();

            final Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setPrefix("de.volkerfaas.arc.public.");
            topic.setDescription("This is the topic for the UserUpdated event.");
            topic.setValueSchemaFile("events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.avsc");
            topic.setNumPartitions(2);

            final Set<ConstraintViolation<Topic>> violations = validator.validate(topic);
            assertNotNull(violations);
            assertEquals(1, violations.size());

            final ConstraintViolation<Topic> violation = violations.stream().findFirst().orElse(null);
            assertNotNull(violation);
            assertEquals("'de.volkerfaas.arc.public.user_updated' has not a valid partition incrementation.", violation.getMessage());
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "compact",
                "delete"
        })
        @DisplayName("should pass if the topic has config parameter 'cleanupPolicy' set to 'compact' or 'delete'")
        void testTopicValidationCleanupPolicy(String cleanupPolicy) {
            final ValidatorPayload validatorPayload = new ValidatorPayload(Collections.emptySet(), Collections.emptyList());
            final Validator validator = validatorFactory.usingContext().constraintValidatorPayload(validatorPayload).getValidator();

            final Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setDescription("This is the topic for the UserUpdated event.");
            topic.setValueSchemaFile("events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.avsc");
            topic.getConfig().put(ApplicationConfiguration.TOPIC_CONFIG_KEY_CLEANUP_POLICY, cleanupPolicy);

            final Set<ConstraintViolation<Topic>> violations = validator.validate(topic);
            assertNotNull(violations);
            assertEquals(0, violations.size());
        }

        @Test
        @DisplayName("should fail if the topic has invalid config parameter 'cleanupPolicy'")
        void testTopicValidationCleanupPolicyInvalid() {
            final ValidatorPayload validatorPayload = new ValidatorPayload(Collections.emptySet(), Collections.emptyList());
            final Validator validator = validatorFactory.usingContext().constraintValidatorPayload(validatorPayload).getValidator();

            final Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setDescription("This is the topic for the UserUpdated event.");
            topic.setValueSchemaFile("events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.avsc");
            topic.getConfig().put(ApplicationConfiguration.TOPIC_CONFIG_KEY_CLEANUP_POLICY, "foo");

            final Set<ConstraintViolation<Topic>> violations = validator.validate(topic);
            assertNotNull(violations);
            assertEquals(1, violations.size());

            final ConstraintViolation<Topic> violation = violations.stream().findFirst().orElse(null);
            assertNotNull(violation);
            assertEquals("must be either compact or delete", violation.getMessage());
        }

    }

}
