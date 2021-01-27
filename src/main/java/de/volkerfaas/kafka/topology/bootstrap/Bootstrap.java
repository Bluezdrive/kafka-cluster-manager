package de.volkerfaas.kafka.topology.bootstrap;

import de.volkerfaas.kafka.topology.KafkaClusterManagerCommandLineArgument;
import de.volkerfaas.utils.CommandLineArguments;
import de.volkerfaas.utils.DefaultCommandLineArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

public final class Bootstrap {

    private static final String[] PROPERTIES = new String[] {
            "BOOTSTRAP_SERVER",
            "CLUSTER_API_KEY",
            "CLUSTER_API_SECRET",
            "SCHEMA_REGISTRY_URL",
            "SCHEMA_REGISTRY_API_KEY",
            "SCHEMA_REGISTRY_API_SECRET"
    };
    private static final Logger LOGGER = LoggerFactory.getLogger(Bootstrap.class);

    private final CommandLineArguments commandLineArguments;
    private final ConfigurableEnvironment environment;
    private boolean result;

    public static Bootstrap init(final String[] args, final ConfigurableEnvironment environment) {
        return new Bootstrap(new DefaultCommandLineArguments(args), environment);
    }

    private Bootstrap(final CommandLineArguments commandLineArguments, final ConfigurableEnvironment environment) {
        this.commandLineArguments = commandLineArguments;
        this.environment = environment;
        this.result = true;
    }

    public boolean containsProperties(final String... keys) {
        return Arrays.stream(keys)
                .map(this::containsProperty)
                .reduce(true, (previous, current) -> previous && current);
    }

    public boolean containsProperty(final String key) {
        final boolean containsProperty = environment.containsProperty(key);
        if (!containsProperty) {
            LOGGER.error("Property '{}' not set!", key);
        }
        return containsProperty;
    }

    public Bootstrap handleArgumentCluster() {
        final String cluster = commandLineArguments.getOptionValue(KafkaClusterManagerCommandLineArgument.CLUSTER);
        if (Objects.isNull(cluster) || cluster.isEmpty()) {
            this.result = false;
            return this;
        }
        final String resourceLocation = environment.getProperty("config.resource") + cluster + ".yaml";
        try {
            final File file = ResourceUtils.getFile(resourceLocation);
            if (!file.exists() && !Objects.equals("local", cluster)) {
                LOGGER.warn("Config file '{}' for cluster '{}' not found.", file, cluster);
                final boolean containsProperties = containsProperties(PROPERTIES);
                if (!containsProperties) {
                    this.result = false;
                }
            }
            environment.setActiveProfiles(cluster);
        } catch (FileNotFoundException e) {
            LOGGER.error("Config filename '{}' for cluster '{}' not specified: {}", resourceLocation, cluster, e.getMessage());
            this.result = false;
        }
        return this;
    }

    public Bootstrap handleArgumentDirectory() {
        final String directory = commandLineArguments.getOptionValue(KafkaClusterManagerCommandLineArgument.DIRECTORY);
        final File path = new File(directory).getAbsoluteFile();
        if (!path.exists() || !path.isDirectory()) {
            LOGGER.error("Directory '{}' doesn't exist.", directory);
            this.result = false;
        }
        return this;
    }

    public boolean result() {
        return result;
    }

    public Bootstrap validateArguments() {
        final Set<String> commandLineArgs = commandLineArguments.getOptionNames();
        final Boolean argumentsValid = commandLineArgs.stream()
                .map(flag -> KafkaClusterManagerCommandLineArgument.isValid(flag, commandLineArgs))
                .reduce(true, (previous, current) -> previous && current);
        final Boolean mandatoryFlagsSet = Arrays.stream(KafkaClusterManagerCommandLineArgument.values())
                .map(commandLineArgument -> commandLineArgument.isMandatorySet(commandLineArgs))
                .reduce(true, (previous, current) -> previous && current);
        if (!argumentsValid || !mandatoryFlagsSet) {
            System.out.println(Help.TEXT_HELP);
            this.result = false;
        }
        return this;
    }

}
