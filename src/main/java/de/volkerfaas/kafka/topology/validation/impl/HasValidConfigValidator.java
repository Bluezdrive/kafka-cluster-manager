package de.volkerfaas.kafka.topology.validation.impl;

import de.volkerfaas.kafka.cluster.model.TopicConfiguration;
import de.volkerfaas.kafka.topology.ApplicationConfiguration;
import de.volkerfaas.kafka.topology.model.Topic;
import de.volkerfaas.kafka.topology.validation.HasValidConfig;
import org.hibernate.validator.constraintvalidation.HibernateConstraintValidatorContext;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class HasValidConfigValidator implements ConstraintValidator<HasValidConfig, Topic> {

    @Override
    public void initialize(HasValidConfig annotation) {
    }

    @Override
    public boolean isValid(final Topic topic, final ConstraintValidatorContext context) {
        context.disableDefaultConstraintViolation();
        final Map<String, String> config = topic.getConfig();
        if (config.containsKey(ApplicationConfiguration.TOPIC_CONFIG_KEY_CLEANUP_POLICY)) {
            final String cleanupPolicy = config.get(ApplicationConfiguration.TOPIC_CONFIG_KEY_CLEANUP_POLICY);
            if (!cleanupPolicy.matches("^(compact|delete)$")) {
                context.buildConstraintViolationWithTemplate("must be either compact or delete")
                        .addPropertyNode(ApplicationConfiguration.TOPIC_CONFIG_KEY_CLEANUP_POLICY)
                        .addConstraintViolation();
                return false;
            }
            final Collection<TopicConfiguration> topicConfigurations = getTopicConfigurations(context);
            if (isNewTopic(topic, topicConfigurations)) {
                return true;
            }
            final String currentCleanupPolicy = topicConfigurations.stream()
                    .filter(topicConfiguration -> topicConfiguration.getName().equals(topic.getFullName()))
                    .map(TopicConfiguration::getConfig)
                    .findFirst()
                    .orElse(Collections.emptyMap())
                    .get(ApplicationConfiguration.TOPIC_CONFIG_KEY_CLEANUP_POLICY);
            if (("delete".equals(currentCleanupPolicy) || Objects.isNull(currentCleanupPolicy)) && "compact".equals(cleanupPolicy)) {
                context.buildConstraintViolationWithTemplate("must not be altered from delete to compact")
                        .addPropertyNode(ApplicationConfiguration.TOPIC_CONFIG_KEY_CLEANUP_POLICY)
                        .addConstraintViolation();

                return false;
            }
        }

        return true;
    }

    private boolean isNewTopic(Topic topic, Collection<TopicConfiguration> topicConfigurations) {
        return topicConfigurations.stream().noneMatch(topicConfiguration -> topicConfiguration.getName().equals(topic.getFullName()));
    }

    private Collection<TopicConfiguration> getTopicConfigurations(ConstraintValidatorContext context) {
        final ValidatorPayload validatorPayload = context.unwrap(HibernateConstraintValidatorContext.class).getConstraintValidatorPayload(ValidatorPayload.class);
        final Collection<TopicConfiguration> topicConfigurations = validatorPayload.getTopicConfigurations();
        return topicConfigurations;
    }

}
