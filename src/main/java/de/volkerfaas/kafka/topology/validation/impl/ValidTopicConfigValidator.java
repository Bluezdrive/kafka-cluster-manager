package de.volkerfaas.kafka.topology.validation.impl;

import de.volkerfaas.kafka.topology.ApplicationConfiguration;
import de.volkerfaas.kafka.topology.validation.ValidTopicConfig;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.Map;

public class ValidTopicConfigValidator implements ConstraintValidator<ValidTopicConfig, Map<String,String>> {

    @Override
    public void initialize(ValidTopicConfig annotation) {
    }

    @Override
    public boolean isValid(Map<String,String> config, ConstraintValidatorContext context) {
        context.disableDefaultConstraintViolation();
        if (config.containsKey(ApplicationConfiguration.TOPIC_CONFIG_KEY_CLEANUP_POLICY)) {
            final String cleanupPolicy = config.get(ApplicationConfiguration.TOPIC_CONFIG_KEY_CLEANUP_POLICY);
            if (!cleanupPolicy.matches("^(compact|delete)$")) {
                context.buildConstraintViolationWithTemplate("must be either compact or delete")
                        .addPropertyNode(ApplicationConfiguration.TOPIC_CONFIG_KEY_CLEANUP_POLICY)
                        .addConstraintViolation();
                return false;
            }
        }

        return true;
    }

}
