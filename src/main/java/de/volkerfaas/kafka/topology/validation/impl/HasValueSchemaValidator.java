package de.volkerfaas.kafka.topology.validation.impl;

import de.volkerfaas.kafka.topology.model.Topic;
import de.volkerfaas.kafka.topology.model.Visibility;
import de.volkerfaas.kafka.topology.validation.HasValueSchema;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class HasValueSchemaValidator  implements ConstraintValidator<HasValueSchema, Visibility>  {

    private Visibility.Type[] visibilityTypes;

    @Override
    public void initialize(HasValueSchema constraintAnnotation) {
        ConstraintValidator.super.initialize(constraintAnnotation);
        visibilityTypes = constraintAnnotation.visibilities();
    }

    @Override
    public boolean isValid(Visibility visibility, ConstraintValidatorContext context) {
        if (Objects.isNull(visibility)) {
            return true;
        } else if (Objects.isNull(visibilityTypes) || visibilityTypes.length == 0) {
            return false;
        } else if (Arrays.stream(visibilityTypes).noneMatch(visibilityType -> visibility.getType().equals(visibilityType))) {
            return true;
        }

        final List<Topic> topics = visibility.getTopics();
        if (Objects.isNull(topics) || topics.isEmpty()) {
            return true;
        }

        return topics.stream().filter(topic -> Objects.isNull(topic.getValueSchema())).count() == 0L;
    }

}
