package de.volkerfaas.kafka.topology.validation.impl;

import de.volkerfaas.kafka.cluster.model.TopicConfiguration;
import de.volkerfaas.kafka.topology.model.Topic;
import de.volkerfaas.kafka.topology.validation.HasValidPartitionIncrementation;
import org.hibernate.validator.constraintvalidation.HibernateConstraintValidatorContext;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.Objects;

public class HasValidPartitionIncrementationValidator implements ConstraintValidator<HasValidPartitionIncrementation, Topic> {

    @Override
    public void initialize(HasValidPartitionIncrementation annotation) {

    }

    @Override
    public boolean isValid(final Topic topic, final ConstraintValidatorContext context) {
        final ValidatorPayload validatorPayload = context.unwrap(HibernateConstraintValidatorContext.class).getConstraintValidatorPayload(ValidatorPayload.class);
        final TopicConfiguration topicConfiguration = validatorPayload.getTopicConfigurations().stream()
                .filter(t -> Objects.equals(t.getName(), topic.getFullName()))
                .findFirst()
                .orElse(null);
        if (Objects.isNull(topicConfiguration)) {
            return true;
        }

        return topic.getNumPartitions() >= topicConfiguration.getPartitions().size();
    }

}
