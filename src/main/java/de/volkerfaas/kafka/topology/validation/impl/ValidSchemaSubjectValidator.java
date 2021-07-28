package de.volkerfaas.kafka.topology.validation.impl;

import de.volkerfaas.kafka.topology.model.Schema;
import de.volkerfaas.kafka.topology.model.Topic;
import de.volkerfaas.kafka.topology.validation.ValidSchemaSubject;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.Objects;

public class ValidSchemaSubjectValidator implements ConstraintValidator<ValidSchemaSubject, Schema> {

    private String type;

    @Override
    public void initialize(ValidSchemaSubject annotation) {
        this.type = annotation.type();
    }

    @Override
    public boolean isValid(Schema schema, ConstraintValidatorContext context) {
        if (Objects.isNull(schema) || Objects.isNull(schema.getSubject())) {
            return true;
        }
        final Topic topic = schema.getTopic();
        final String subject = topic.getFullName() + "-" + type;
        return Objects.equals(subject, schema.getSubject());
    }

}
