package de.volkerfaas.kafka.topology;

import de.volkerfaas.utils.CommandLineOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static de.volkerfaas.kafka.topology.KafkaClusterManagerCommandLineProperty.*;

public enum KafkaClusterManagerCommandLineOption implements CommandLineOption {

    CREATE("create", List.of(DIRECTORY, DOMAIN, DESCRIPTION, MAINTAINER_NAME, MAINTAINER_EMAIL, SERVICE_ACCOUNT_ID), List.of(DOMAIN, DESCRIPTION, MAINTAINER_NAME, MAINTAINER_EMAIL, SERVICE_ACCOUNT_ID)),
    DELETE("delete", List.of(DIRECTORY, DOMAIN), List.of(DOMAIN)),
    DEPLOY("deploy", List.of(DIRECTORY, DOMAIN, DRY_RUN, CLUSTER, ALLOW_DELETE_ACL, ALLOW_DELETE_SUBJECTS, ALLOW_DELETE_TOPICS), Collections.emptyList()),
    RESTORE("restore", List.of(DIRECTORY, DOMAIN, DRY_RUN, CLUSTER), List.of(DOMAIN));

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClusterManagerCommandLineOption.class);

    public static boolean isValid(final String name, final Set<String> propertyNames) {
        final KafkaClusterManagerCommandLineOption option = valueByName(name);
        if (Objects.isNull(option)) {
            LOGGER.error("Option '--{}' not allowed", name);
            return false;
        }

        return option.isValid(propertyNames) && option.isEveryMandatoryPropertySet(propertyNames);
    }

    static KafkaClusterManagerCommandLineOption valueByName(final String name) {
        return Arrays.stream(KafkaClusterManagerCommandLineOption.values())
                .filter(option -> Objects.equals(option.name, name))
                .findFirst().orElse(null);
    }

    private final String name;
    private final List<KafkaClusterManagerCommandLineProperty> allowedProperties;
    private final List<KafkaClusterManagerCommandLineProperty> mandatoryProperties;

    KafkaClusterManagerCommandLineOption(String name, List<KafkaClusterManagerCommandLineProperty> allowedProperties, List<KafkaClusterManagerCommandLineProperty> mandatoryProperties) {
        this.name = name;
        this.allowedProperties = allowedProperties;
        this.mandatoryProperties = mandatoryProperties;
    }

    @Override
    public String getName() {
        return name;
    }

    public boolean isValid(final Set<String> commandLineArguments) {
        return commandLineArguments.stream()
                .map(this::isAllowedProperty)
                .reduce(true, (previous, current) -> previous && current);
    }

    public boolean isAllowedProperty(final String propertyName) {
        final KafkaClusterManagerCommandLineProperty commandLineProperty = KafkaClusterManagerCommandLineProperty.valueByName(propertyName);
        if (Objects.isNull(commandLineProperty)) {
            return false;
        }

        return allowedProperties.contains(commandLineProperty);
    }

    public boolean isEveryMandatoryPropertySet(final Set<String> propertyNames) {
        return mandatoryProperties.stream()
                .map(KafkaClusterManagerCommandLineProperty::getName)
                .map(propertyNames::contains)
                .reduce(true, (previous, current) -> previous && current);
    }

}
