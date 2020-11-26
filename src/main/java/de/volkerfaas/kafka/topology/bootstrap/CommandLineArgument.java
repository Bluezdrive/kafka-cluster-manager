package de.volkerfaas.kafka.topology.bootstrap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public enum CommandLineArgument {

    HELP("help", Collections.emptyList(), Collections.emptyList()),
    DIRECTORY("directory", Collections.emptyList(), Collections.emptyList()),
    DOMAIN("domain", Collections.emptyList(), Collections.emptyList()),
    CONFIG("config", Collections.emptyList(), Collections.emptyList()),
    ALLOW_DELETE_ACL("allow-delete-acl", Collections.emptyList(), List.of(DOMAIN)),
    ALLOW_DELETE_TOPICS("allow-delete-topics", Collections.emptyList(), List.of(DOMAIN)),
    DRY_RUN("dry-run", Collections.emptyList(), Collections.emptyList()),
    RESTORE("restore", List.of(DOMAIN), Collections.emptyList());

    private static final Logger LOGGER = LoggerFactory.getLogger(CommandLineArgument.class);

    public static boolean isValid(final String flag, List<String> commandLineArguments) {
        final CommandLineArgument commandLineArgument = valueByFlag(flag);
        if (Objects.isNull(commandLineArgument)) {
            LOGGER.error("Flag '--{}' not allowed", flag);
            return false;
        }

        return commandLineArgument.isValid(commandLineArguments);
    }

    private static CommandLineArgument valueByFlag(final String flag) {
        return Arrays.stream(CommandLineArgument.values())
                .filter(argument -> Objects.equals(argument.flag, flag))
                .findFirst().orElse(null);
    }

    private final String flag;
    private final List<CommandLineArgument> requires;
    private final List<CommandLineArgument> excludes;

    CommandLineArgument(final String flag, final List<CommandLineArgument> requires, final List<CommandLineArgument> excludes) {
        this.flag = flag;
        this.requires = requires;
        this.excludes = excludes;
    }

    public String getFlag() {
        return flag;
    }

    public boolean isValid(List<String> commandLineArguments) {
        final Boolean resultRequires = requires.stream()
                .map(requiredCommandLineArgument -> isRequiredFlagSet(requiredCommandLineArgument, commandLineArguments))
                .reduce(true, (previous, current) -> previous && current);
        final Boolean resultExcludes = excludes.stream()
                .map(excludedCommandLineArgument -> isExcludedFlagNotSet(excludedCommandLineArgument, commandLineArguments))
                .reduce(true, (previous, current) -> previous && current);
        return resultRequires && resultExcludes;
    }

    private boolean isRequiredFlagSet(CommandLineArgument requiredCommandLineArgument, List<String> commandLineArguments) {
        final String requiredFlag = requiredCommandLineArgument.getFlag();
        final boolean contains = commandLineArguments.contains(requiredFlag);
        if (!contains) {
            LOGGER.error("Flag '--{}' must be set in combination with '--{}'", requiredFlag, flag);
        }
        return contains;
    }

    private boolean isExcludedFlagNotSet(CommandLineArgument excludedCommandLineArgument, List<String> commandLineArguments) {
        final String excludedFlag = excludedCommandLineArgument.getFlag();
        final boolean contains = commandLineArguments.contains(excludedFlag);
        if (contains) {
            LOGGER.error("Flag '--{}' must not be set in combination with '--{}'", excludedFlag, flag);
        }
        return !contains;
    }

}
