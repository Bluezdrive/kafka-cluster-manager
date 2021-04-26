package de.volkerfaas.kafka.topology.validation.impl;

import de.volkerfaas.kafka.topology.model.Schema;
import de.volkerfaas.kafka.topology.model.Topic;
import de.volkerfaas.kafka.topology.validation.ValidSchemaSubject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@DisplayName("The ValidSchemaSubjectValidator")
public class ValidSchemaSubjectValidatorTest {

    @ParameterizedTest
    @ValueSource(strings = { "key", "value" })
    @DisplayName("should return true if the topic name matches the schema subject")
    void testValidSchemaSubject(String type) {
        final ValidSchemaSubject annotation = mock(ValidSchemaSubject.class);
        doReturn(type).when(annotation).type();

        final Topic topic = new Topic();
        topic.setPrefix("de.volkerfaas.arc.public.");
        topic.setName("user_updated");
        topic.setDescription("This is the topic for the UserUpdated event.");
        topic.getClusters().add("test");
        final String subject = topic.getFullName() + "-" + type;
        final Schema valueSchema = new Schema(subject, Schema.Type.AVRO, Schema.CompatibilityMode.FORWARD_TRANSITIVE);
        valueSchema.setTopic(topic);
        topic.setValueSchema(valueSchema);

        final ValidSchemaSubjectValidator validator = new ValidSchemaSubjectValidator();
        validator.initialize(annotation);
        final boolean valid = validator.isValid(valueSchema, null);
        assertTrue(valid);
    }

    @ParameterizedTest
    @ValueSource(strings = { "key", "value" })
    @DisplayName("should return false if the topic name does not match the schema subject")
    void testNotValidSchemaSubject(String type) {
        final ValidSchemaSubject annotation = mock(ValidSchemaSubject.class);
        doReturn(type).when(annotation).type();

        final Topic topic = new Topic();
        topic.setPrefix("de.volkerfaas.arc.public.");
        topic.setName("user_updated");
        topic.setDescription("This is the topic for the UserUpdated event.");
        topic.getClusters().add("test");
        final String subject = "de.volkerfaas.test.public.user_updated" + "-" + type;
        final Schema valueSchema = new Schema(subject, Schema.Type.AVRO, Schema.CompatibilityMode.FORWARD_TRANSITIVE);
        valueSchema.setTopic(topic);
        topic.setValueSchema(valueSchema);

        final ValidSchemaSubjectValidator validator = new ValidSchemaSubjectValidator();
        validator.initialize(annotation);
        final boolean valid = validator.isValid(valueSchema, null);
        assertFalse(valid);
    }

}
