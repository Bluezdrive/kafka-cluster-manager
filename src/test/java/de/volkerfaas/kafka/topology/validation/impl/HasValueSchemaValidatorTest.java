package de.volkerfaas.kafka.topology.validation.impl;

import de.volkerfaas.kafka.topology.model.Schema;
import de.volkerfaas.kafka.topology.model.Topic;
import de.volkerfaas.kafka.topology.model.Visibility;
import de.volkerfaas.kafka.topology.validation.HasValueSchema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@DisplayName("The HasValueSchemaValidatorTest")
public class HasValueSchemaValidatorTest {

    @ParameterizedTest
    @EnumSource(Visibility.Type.class)
    @DisplayName("should return true if all topics in the given visibility do have a value schema")
    void testCouldHaveValueSchema(Visibility.Type type) {
        final HasValueSchema annotation = mock(HasValueSchema.class);
        doReturn(new Visibility.Type[] {Visibility.Type.PUBLIC, Visibility.Type.PROTECTED}).when(annotation).visibilities();

        Visibility visibility = new Visibility(type);

        final Topic topicUserCreated = new Topic();
        topicUserCreated.setPrefix("de.volkerfaas.arc." + type.getValue().toLowerCase(Locale.ROOT) + ".");
        topicUserCreated.setName("user_created");
        topicUserCreated.setDescription("This is the topic for the UserCreated event.");
        topicUserCreated.getClusters().add("test");
        final String subjectUserCreated = "de.volkerfaas.test." + type.getValue().toLowerCase(Locale.ROOT) + ".user_created" + "-" + type;
        final Schema valueSchemaUserCreated = new Schema(subjectUserCreated, Schema.Type.AVRO, Schema.CompatibilityMode.FORWARD_TRANSITIVE);
        valueSchemaUserCreated.setTopic(topicUserCreated);
        topicUserCreated.setValueSchema(valueSchemaUserCreated);
        visibility.getTopics().add(topicUserCreated);

        final Topic topicUserUpdated = new Topic();
        topicUserUpdated.setPrefix("de.volkerfaas.arc." + type.getValue().toLowerCase(Locale.ROOT) + ".");
        topicUserUpdated.setName("user_updated");
        topicUserUpdated.setDescription("This is the topic for the UserUpdated event.");
        topicUserUpdated.getClusters().add("test");
        final String subjectUserUpdated = "de.volkerfaas.test." + type.getValue().toLowerCase(Locale.ROOT) + ".user_updated" + "-" + type;
        final Schema valueSchemaUserUpdated = new Schema(subjectUserUpdated, Schema.Type.AVRO, Schema.CompatibilityMode.FORWARD_TRANSITIVE);
        valueSchemaUserUpdated.setTopic(topicUserUpdated);
        topicUserUpdated.setValueSchema(valueSchemaUserUpdated);
        visibility.getTopics().add(topicUserUpdated);

        final HasValueSchemaValidator validator = new HasValueSchemaValidator();
        validator.initialize(annotation);
        final boolean valid = validator.isValid(visibility, null);
        assertTrue(valid);
    }

    @ParameterizedTest
    @EnumSource(value = Visibility.Type.class, names = {"PUBLIC", "PROTECTED"})
    @DisplayName("should return false if a topic in the given visibility does not have a value schema")
    void testMustHaveValueSchema(Visibility.Type type) {
        final HasValueSchema annotation = mock(HasValueSchema.class);
        doReturn(new Visibility.Type[] {Visibility.Type.PUBLIC, Visibility.Type.PROTECTED}).when(annotation).visibilities();

        Visibility visibility = new Visibility(type);

        final Topic topicWithValueSchema = new Topic();
        topicWithValueSchema.setPrefix("de.volkerfaas.arc." + type.getValue().toLowerCase(Locale.ROOT) + ".");
        topicWithValueSchema.setName("user_created");
        topicWithValueSchema.setDescription("This is the topic for the UserCreated event.");
        topicWithValueSchema.getClusters().add("test");
        final String subject = "de.volkerfaas.test." + type.getValue().toLowerCase(Locale.ROOT) + ".user_created" + "-" + type;
        final Schema valueSchema = new Schema(subject, Schema.Type.AVRO, Schema.CompatibilityMode.FORWARD_TRANSITIVE);
        valueSchema.setTopic(topicWithValueSchema);
        topicWithValueSchema.setValueSchema(valueSchema);
        visibility.getTopics().add(topicWithValueSchema);

        final Topic topicWithoutValueSchema = new Topic();
        topicWithoutValueSchema.setPrefix("de.volkerfaas.arc." + type.getValue().toLowerCase(Locale.ROOT) + ".");
        topicWithoutValueSchema.setName("user_updated");
        topicWithoutValueSchema.setDescription("This is the topic for the UserUpdated event.");
        topicWithoutValueSchema.getClusters().add("test");
        visibility.getTopics().add(topicWithoutValueSchema);

        final HasValueSchemaValidator validator = new HasValueSchemaValidator();
        validator.initialize(annotation);
        final boolean valid = validator.isValid(visibility, null);
        assertFalse(valid);
    }

    @ParameterizedTest
    @EnumSource(value = Visibility.Type.class, names = {"PRIVATE"})
    @DisplayName("should return true if a topic in the given visibility does not have a value schema")
    void testMustNotHaveValueSchema(Visibility.Type type) {
        final HasValueSchema annotation = mock(HasValueSchema.class);
        doReturn(new Visibility.Type[] {Visibility.Type.PUBLIC, Visibility.Type.PROTECTED}).when(annotation).visibilities();

        Visibility visibility = new Visibility(type);

        final Topic topicWithValueSchema = new Topic();
        topicWithValueSchema.setPrefix("de.volkerfaas.arc." + type.getValue().toLowerCase(Locale.ROOT) + ".");
        topicWithValueSchema.setName("user_created");
        topicWithValueSchema.setDescription("This is the topic for the UserCreated event.");
        topicWithValueSchema.getClusters().add("test");
        final String subject = "de.volkerfaas.test." + type.getValue().toLowerCase(Locale.ROOT) + ".user_created" + "-" + type;
        final Schema valueSchema = new Schema(subject, Schema.Type.AVRO, Schema.CompatibilityMode.FORWARD_TRANSITIVE);
        valueSchema.setTopic(topicWithValueSchema);
        topicWithValueSchema.setValueSchema(valueSchema);
        visibility.getTopics().add(topicWithValueSchema);

        final Topic topicWithoutValueSchema = new Topic();
        topicWithoutValueSchema.setPrefix("de.volkerfaas.arc." + type.getValue().toLowerCase(Locale.ROOT) + ".");
        topicWithoutValueSchema.setName("user_updated");
        topicWithoutValueSchema.setDescription("This is the topic for the UserUpdated event.");
        topicWithoutValueSchema.getClusters().add("test");
        visibility.getTopics().add(topicWithoutValueSchema);

        final HasValueSchemaValidator validator = new HasValueSchemaValidator();
        validator.initialize(annotation);
        final boolean valid = validator.isValid(visibility, null);
        assertTrue(valid);
    }

}
