package de.volkerfaas.kafka.topology.bootstrap;

import de.volkerfaas.kafka.topology.KafkaClusterManagerCommandLineOption;
import de.volkerfaas.kafka.topology.KafkaClusterManagerCommandLineProperty;
import de.volkerfaas.utils.CommandLineArguments;
import de.volkerfaas.utils.DefaultCommandLineArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public final class Bootstrap {

    private static final Logger LOGGER = LoggerFactory.getLogger(Bootstrap.class);

    private final CommandLineArguments commandLineArguments;
    private boolean result;

    public static Bootstrap init(final String[] args) {
        return new Bootstrap(new DefaultCommandLineArguments(args));
    }

    private Bootstrap(final CommandLineArguments commandLineArguments) {
        this.commandLineArguments = commandLineArguments;
        this.result = true;
    }

    public Bootstrap handleArgumentDirectory() {
        final String directory = commandLineArguments.getPropertyValue(KafkaClusterManagerCommandLineProperty.DIRECTORY);
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
        final List<String> nonOptionArgs = commandLineArguments.getOptions();
        if (Objects.isNull(nonOptionArgs) || nonOptionArgs.isEmpty()) {
            System.out.println(Help.TEXT_HELP);
            this.result = false;
            return this;
        }
        final String optionName = nonOptionArgs.get(0);
        final Set<String> commandLinePropertyNames = commandLineArguments.getPropertyNames();
        final boolean optionValid = KafkaClusterManagerCommandLineOption.isValid(optionName, commandLinePropertyNames);
        final Boolean propertiesValid = commandLinePropertyNames.stream()
                .map(propertyName -> KafkaClusterManagerCommandLineProperty.isValid(propertyName, commandLinePropertyNames))
                .reduce(true, (previous, current) -> previous && current);
        if (!optionValid || !propertiesValid) {
            System.out.println(Help.TEXT_HELP);
            this.result = false;
        }
        return this;
    }

}
