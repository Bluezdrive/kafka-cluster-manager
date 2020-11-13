package de.volkerfaas.kafka.topology.model;

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
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
    class A_topology_file {

        @Test
        void name_must_match_the_specification() {
            TopologyFile topology = new TopologyFile();
            topology.setFile(new File(directory, "topology-de.volkerfaas.arc.yaml"));

            Team maintainer = new Team();
            maintainer.setName("Volker Faas");
            maintainer.setEmail("bluezdrive@volkerfaas.de");

            Domain domain = new Domain();
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
        void name_must_not_be_null() {
            TopologyFile topology = new TopologyFile();

            final Validator validator = validatorFactory.getValidator();
            final Set<ConstraintViolation<TopologyFile>> violations = validator.validate(topology);
            assertNotNull(violations);
            assertEquals(1, violations.size());

            final ConstraintViolation<TopologyFile> violation = violations.stream().findFirst().orElse(null);
            assertNotNull(violation);
            assertEquals("must not be null", violation.getMessage());
        }

        @Test
        void name_must_have_domain_name_in_filename() {
            TopologyFile topology = new TopologyFile();
            topology.setFile(new File(directory, "topology-de.volkerfaas.foo.yaml"));

            Team maintainer = new Team();
            maintainer.setName("Volker Faas");
            maintainer.setEmail("bluezdrive@volkerfaas.de");

            Domain domain = new Domain();
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
        void name_must_not_have_an_invalid_domainname(String filename) {
            TopologyFile topology = new TopologyFile();
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
    class A_domain {

        @Test
        void must_match_the_specification() {
            Team maintainer = new Team();
            maintainer.setName("Volker Faas");
            maintainer.setEmail("bluezdrive@volkerfaas.de");

            Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            domain.setDescription("Domain for testing architecture stuff with Apache Kafka® cluster.");
            domain.setPrincipal("User:129849");
            domain.setMaintainer(maintainer);

            Visibility publicVisibility = new Visibility();
            publicVisibility.setType(Visibility.Type.PUBLIC);
            domain.getVisibilities().add(publicVisibility);

            Visibility protectedVisibility = new Visibility();
            protectedVisibility.setType(Visibility.Type.PROTECTED);
            domain.getVisibilities().add(protectedVisibility);

            Visibility privateVisibility = new Visibility();
            privateVisibility.setType(Visibility.Type.PRIVATE);
            domain.getVisibilities().add(privateVisibility);

            final Validator validator = validatorFactory.getValidator();
            final Set<ConstraintViolation<Domain>> violations = validator.validate(domain);
            assertEquals(0, violations.size());
        }

        @Test
        void name_must_not_be_null() {
            Team maintainer = new Team();
            maintainer.setName("Volker Faas");
            maintainer.setEmail("bluezdrive@volkerfaas.de");

            Domain domain = new Domain();
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
        void description_must_not_be_blank(String description) {
            Team maintainer = new Team();
            maintainer.setName("Volker Faas");
            maintainer.setEmail("bluezdrive@volkerfaas.de");

            Domain domain = new Domain();
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
        void name_must_not_be_empty_or_blank(String name) {
            Team maintainer = new Team();
            maintainer.setName("Volker Faas");
            maintainer.setEmail("bluezdrive@volkerfaas.de");

            Domain domain = new Domain();
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
        void principal_must_not_be_null() {
            Team maintainer = new Team();
            maintainer.setName("Volker Faas");
            maintainer.setEmail("bluezdrive@volkerfaas.de");

            Domain domain = new Domain();
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
        void principal_must_not_violate_the_specification(String principal) {
            Team maintainer = new Team();
            maintainer.setName("Volker Faas");
            maintainer.setEmail("bluezdrive@volkerfaas.de");

            Domain domain = new Domain();
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
        void must_not_have_more_than_one_visibility_of_each_type(String type) {
            Team maintainer = new Team();
            maintainer.setName("Volker Faas");
            maintainer.setEmail("bluezdrive@volkerfaas.de");

            Domain domain = new Domain();
            domain.setName("de.volkerfaas.arc");
            domain.setPrincipal("User:129849");
            domain.setDescription("Domain for testing architecture stuff with Apache Kafka® cluster.");
            domain.setMaintainer(maintainer);

            Visibility publicVisibility = new Visibility();
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
    class VisibilityValidation {



    }

    @Nested
    class A_topic {

        @Test
        void must_match_the_specification() {
            final ValidatorPayload validatorPayload = new ValidatorPayload(Collections.emptySet(), Collections.emptyList());
            final Validator validator = validatorFactory.usingContext().constraintValidatorPayload(validatorPayload).getValidator();

            Topic topic = new Topic();
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
        void number_of_partitions_must_not_be_less_than_one() {
            final ValidatorPayload validatorPayload = new ValidatorPayload(Collections.emptySet(), Collections.emptyList());
            final Validator validator = validatorFactory.usingContext().constraintValidatorPayload(validatorPayload).getValidator();

            Topic topic = new Topic();
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
        void number_of_partitions_must_not_be_greater_than_twenty() {
            final ValidatorPayload validatorPayload = new ValidatorPayload(Collections.emptySet(), Collections.emptyList());
            final Validator validator = validatorFactory.usingContext().constraintValidatorPayload(validatorPayload).getValidator();

            Topic topic = new Topic();
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
        void new_number_of_partitions_must_be_greater_than_existing() {
            final List<KafkaTopic> kafkaTopics = List.of(new KafkaTopic("de.volkerfaas.arc.public.user_updated", 4, (short) 3, Collections.emptyMap()));
            final ValidatorPayload validatorPayload = new ValidatorPayload(Collections.emptySet(), kafkaTopics);
            final Validator validator = validatorFactory.usingContext().constraintValidatorPayload(validatorPayload).getValidator();

            Topic topic = new Topic();
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
        void new_number_of_partitions_must_not_be_less_than_existing() {
            final List<KafkaTopic> kafkaTopics = List.of(new KafkaTopic("de.volkerfaas.arc.public.user_updated", 4, (short) 3, Collections.emptyMap()));
            final ValidatorPayload validatorPayload = new ValidatorPayload(Collections.emptySet(), kafkaTopics);
            final Validator validator = validatorFactory.usingContext().constraintValidatorPayload(validatorPayload).getValidator();

            Topic topic = new Topic();
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
        void might_contain_config_parameter_cleanup_policy(String cleanupPolicy) {
            final ValidatorPayload validatorPayload = new ValidatorPayload(Collections.emptySet(), Collections.emptyList());
            final Validator validator = validatorFactory.usingContext().constraintValidatorPayload(validatorPayload).getValidator();

            Topic topic = new Topic();
            topic.setName("user_updated");
            topic.setDescription("This is the topic for the UserUpdated event.");
            topic.setValueSchemaFile("events/de.volkerfaas.arc/de.volkerfaas.arc.public.user_updated-value.avsc");
            topic.getConfig().put(ApplicationConfiguration.TOPIC_CONFIG_KEY_CLEANUP_POLICY, cleanupPolicy);

            final Set<ConstraintViolation<Topic>> violations = validator.validate(topic);
            assertNotNull(violations);
            assertEquals(0, violations.size());
        }

        @Test
        void must_not_contain_config_parameter_cleanup_policy_with_invalid_value() {
            final ValidatorPayload validatorPayload = new ValidatorPayload(Collections.emptySet(), Collections.emptyList());
            final Validator validator = validatorFactory.usingContext().constraintValidatorPayload(validatorPayload).getValidator();

            Topic topic = new Topic();
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
