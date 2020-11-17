package de.volkerfaas.kafka.topology.validation.impl;

import de.volkerfaas.kafka.cluster.model.TopicConfiguration;
import de.volkerfaas.kafka.topology.model.Topic;
import de.volkerfaas.kafka.topology.validation.HasValidPartitionIncrementation;
import de.volkerfaas.kafka.topology.validation.ValidatorPayload;
import org.hibernate.validator.constraintvalidation.HibernateConstraintValidatorContext;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.Objects;

public class HasValidPartitionIncrementationValidator implements ConstraintValidator<HasValidPartitionIncrementation, Topic> {

    @Override
    public void initialize(HasValidPartitionIncrementation annotation) {

    }

    @Override
    public boolean isValid(Topic topic, ConstraintValidatorContext context) {
        final ValidatorPayload validatorPayload = context.unwrap(HibernateConstraintValidatorContext.class).getConstraintValidatorPayload(ValidatorPayload.class);
        final TopicConfiguration topicConfiguration = validatorPayload.getKafkaTopics().stream()
                .filter(t -> Objects.equals(t.getName(), topic.getFullName()))
                .findFirst()
                .orElse(null);
        if (Objects.isNull(topicConfiguration)) {
            return true;
        }

        return topic.getNumPartitions() >= topicConfiguration.getPartitions().size();
    }

}
