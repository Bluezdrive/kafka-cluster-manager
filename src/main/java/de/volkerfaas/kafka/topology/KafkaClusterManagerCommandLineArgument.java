package de.volkerfaas.kafka.topology;

import de.volkerfaas.utils.CommandLineArgument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public enum KafkaClusterManagerCommandLineArgument implements CommandLineArgument {

    HELP("help", false, Collections.emptyList(), Collections.emptyList()),
    DIRECTORY("directory", false, Collections.emptyList(), Collections.emptyList(), "topology"),
    DOMAIN("domain", false, Collections.emptyList(), Collections.emptyList()),
    CLUSTER("cluster", false, Collections.emptyList(), Collections.emptyList(), "local"),
    ALLOW_DELETE_ACL("allow-delete-acl", false, Collections.emptyList(), List.of(DOMAIN)),
    ALLOW_DELETE_TOPICS("allow-delete-topics", false, Collections.emptyList(), List.of(DOMAIN)),
    DRY_RUN("dry-run", false, Collections.emptyList(), Collections.emptyList()),
    RESTORE("restore", false, List.of(DOMAIN), Collections.emptyList());

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClusterManagerCommandLineArgument.class);

    private final String name;
    private final boolean mandatory;
    private final List<KafkaClusterManagerCommandLineArgument> requires;
    private final List<KafkaClusterManagerCommandLineArgument> excludes;
    private final String defaultValue;

    public static boolean isValid(final String name, final Set<String> commandLineArguments) {
        final KafkaClusterManagerCommandLineArgument commandLineArgument = valueByName(name);
        if (Objects.isNull(commandLineArgument)) {
            LOGGER.error("Option '--{}' not allowed", name);
            return false;
        }

        return commandLineArgument.isValid(commandLineArguments);
    }

    static KafkaClusterManagerCommandLineArgument valueByName(final String name) {
        return Arrays.stream(KafkaClusterManagerCommandLineArgument.values())
                .filter(argument -> Objects.equals(argument.name, name))
                .findFirst().orElse(null);
    }

    KafkaClusterManagerCommandLineArgument(final String name, final boolean mandatory, final List<KafkaClusterManagerCommandLineArgument> requires, final List<KafkaClusterManagerCommandLineArgument> excludes) {
        this(name, mandatory, requires, excludes, null);
    }

    KafkaClusterManagerCommandLineArgument(final String name, final boolean mandatory, final List<KafkaClusterManagerCommandLineArgument> requires, final List<KafkaClusterManagerCommandLineArgument> excludes, String defaultValue) {
        this.name = name;
        this.mandatory = mandatory;
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
                .map(requiredCommandLineArgument -> isRequiredOptionSet(requiredCommandLineArgument, commandLineArguments))
                .reduce(true, (previous, current) -> previous && current);
        final Boolean resultExcludes = excludes.stream()
                .map(excludedCommandLineArgument -> isExcludedOptionNotSet(excludedCommandLineArgument, commandLineArguments))
                .reduce(true, (previous, current) -> previous && current);
        return resultRequires && resultExcludes;
    }

    public boolean isMandatorySet(final Set<String> commandLineArguments) {
        if (mandatory) {
            final boolean contains = commandLineArguments.contains(name);
            if (!contains) {
                LOGGER.error("Option '--{}' must be set", name);
            }
            return contains;
        } else {
            return true;
        }
    }

    private boolean isRequiredOptionSet(KafkaClusterManagerCommandLineArgument requiredCommandLineArgument, Set<String> commandLineArguments) {
        final String requiredFlag = requiredCommandLineArgument.getName();
        final boolean contains = commandLineArguments.contains(requiredFlag);
        if (!contains) {
            LOGGER.error("Option '--{}' must be set in combination with '--{}'", requiredFlag, name);
        }
        return contains;
    }

    private boolean isExcludedOptionNotSet(KafkaClusterManagerCommandLineArgument excludedCommandLineArgument, Set<String> commandLineArguments) {
        final String excludedFlag = excludedCommandLineArgument.getName();
        final boolean contains = commandLineArguments.contains(excludedFlag);
        if (contains) {
            LOGGER.error("Option '--{}' must not be set in combination with '--{}'", excludedFlag, name);
        }
        return !contains;
    }

}
