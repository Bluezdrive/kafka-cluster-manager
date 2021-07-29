package de.volkerfaas.kafka.topology;

import de.volkerfaas.utils.CommandLineProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public enum KafkaClusterManagerCommandLineProperty implements CommandLineProperty {

    HELP("help", Collections.emptyList(), Collections.emptyList()),
    DIRECTORY("directory", Collections.emptyList(), Collections.emptyList(), "topology"),
    DOMAIN("domain", Collections.emptyList(), Collections.emptyList()),
    DESCRIPTION("description", Collections.emptyList(), Collections.emptyList()),
    MAINTAINER_NAME("maintainer-name", Collections.emptyList(), Collections.emptyList()),
    MAINTAINER_EMAIL("maintainer-email", Collections.emptyList(), Collections.emptyList()),
    SERVICE_ACCOUNT_ID("service-account-id", Collections.emptyList(), Collections.emptyList()),
    CLUSTER("cluster", Collections.emptyList(), Collections.emptyList(), "local"),
    ALLOW_DELETE_ACL("allow-delete-acl", Collections.emptyList(), List.of(DOMAIN)),
    ALLOW_DELETE_SUBJECTS("allow-delete-subjects", Collections.emptyList(), List.of(DOMAIN)),
    ALLOW_DELETE_TOPICS("allow-delete-topics", Collections.emptyList(), List.of(DOMAIN)),
    DRY_RUN("dry-run", Collections.emptyList(), Collections.emptyList())
    ;

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClusterManagerCommandLineProperty.class);

    private final String name;
    private final List<KafkaClusterManagerCommandLineProperty> requires;
    private final List<KafkaClusterManagerCommandLineProperty> excludes;
    private final String defaultValue;

    public static boolean isValid(final String name, final Set<String> commandLineArguments) {
        final KafkaClusterManagerCommandLineProperty commandLineArgument = valueByName(name);
        if (Objects.isNull(commandLineArgument)) {
            LOGGER.error("Option '--{}' not allowed", name);
            return false;
        }

        return commandLineArgument.isValid(commandLineArguments);
    }

    static KafkaClusterManagerCommandLineProperty valueByName(final String name) {
        return Arrays.stream(KafkaClusterManagerCommandLineProperty.values())
                .filter(argument -> Objects.equals(argument.name, name))
                .findFirst().orElse(null);
    }

    KafkaClusterManagerCommandLineProperty(final String name, final List<KafkaClusterManagerCommandLineProperty> requires, final List<KafkaClusterManagerCommandLineProperty> excludes) {
        this(name, requires, excludes, null);
    }

    KafkaClusterManagerCommandLineProperty(final String name, final List<KafkaClusterManagerCommandLineProperty> requires, final List<KafkaClusterManagerCommandLineProperty> excludes, String defaultValue) {
        this.name = name;
        this.requires = requires;
        this.excludes = excludes;
        this.defaultValue = defaultValue;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getDefaultValue() {
        return defaultValue;
    }


    public boolean isValid(final Set<String> commandLineArguments) {
        final Boolean resultRequires = requires.stream()
                .map(requiredCommandLineArgument -> isDependentPropertySet(requiredCommandLineArgument, commandLineArguments))
                .reduce(true, (previous, current) -> previous && current);
        final Boolean resultExcludes = excludes.stream()
                .map(excludedCommandLineArgument -> isExcludedPropertyNotSet(excludedCommandLineArgument, commandLineArguments))
                .reduce(true, (previous, current) -> previous && current);
        return resultRequires && resultExcludes;
    }

    private boolean isDependentPropertySet(KafkaClusterManagerCommandLineProperty dependentCommandLineProperty, Set<String> commandLineArguments) {
        final String dependentPropertyName = dependentCommandLineProperty.getName();
        final boolean contains = commandLineArguments.contains(dependentPropertyName);
        if (!contains) {
            LOGGER.error("Option '--{}' must be set in combination with '--{}'", dependentPropertyName, name);
        }
        return contains;
    }

    private boolean isExcludedPropertyNotSet(KafkaClusterManagerCommandLineProperty excludedCommandLineArgument, Set<String> commandLineArguments) {
        final String excludedFlag = excludedCommandLineArgument.getName();
        final boolean contains = commandLineArguments.contains(excludedFlag);
        if (contains) {
            LOGGER.error("Option '--{}' must not be set in combination with '--{}'", excludedFlag, name);
        }
        return !contains;
    }

}
