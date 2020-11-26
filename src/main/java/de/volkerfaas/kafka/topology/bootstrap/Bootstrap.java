package de.volkerfaas.kafka.topology.bootstrap;

import org.springframework.core.env.CommandLinePropertySource;
import org.springframework.core.env.ConfigurableEnvironment;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class Bootstrap {

    public static final String HELP = System.lineSeparator() +
            "Usage:" + System.lineSeparator() +
            "  java -jar kafka-cluster-manager.jar [flags]" + System.lineSeparator() +
            System.lineSeparator() +
            "Examples:" + System.lineSeparator() +
            "  java -jar kafka-cluster-manager.jar --config=[config] " + System.lineSeparator() +
            "    → Uploads a topology to the cluster with config [config]." + System.lineSeparator() +
            "  java -jar kafka-cluster-manager.jar --config=[config] --domain=[domain]" + System.lineSeparator() +
            "    → Uploads only domain [domain] of topology to the cluster with config [config]." + System.lineSeparator() +
            "  java -jar kafka-cluster-manager.jar --config=[config] --dry-run" + System.lineSeparator() +
            "    → Executes without making changes to the cluster." + System.lineSeparator() +
            "  java -jar kafka-cluster-manager.jar --config=[config] --restore --domain[domain]" + System.lineSeparator() +
            "    → Downloads domain [domain] from the cluster with config [config] to a local topology file." + System.lineSeparator() +
            "  java -jar kafka-cluster-manager.jar --config=[config] --restore --domain[domain] --dry-run" + System.lineSeparator() +
            "    → Executes without making changes to the cluster." + System.lineSeparator() +
            System.lineSeparator() +
            "Available Flags:" + System.lineSeparator() +
            "  --help                     Show help." + System.lineSeparator() +
            "  --directory=[directory]    Set base directory for topology files. Default is \"topology\"." + System.lineSeparator() +
            "  --domain=[domain]          Processes only a single domain" + System.lineSeparator() +
            "  --config=[config]          Sets the property file to use to connect to the cluster" + System.lineSeparator() +
            "  --allow-delete-acl         Allow deletion of orphaned ACLs. Cannot be used in combination with flag --domain. (DO NOT USE IN PRODUCTION!)" + System.lineSeparator() +
            "  --allow-delete-topics      Allow deletion of orphaned topics. Cannot be used in combination with flag --domain. (DO NOT USE IN PRODUCTION!)" + System.lineSeparator() +
            "  --dry-run                  Makes no changes to the remote topology" + System.lineSeparator() +
            "  --restore                  Restores the domains listed with flag --domain into file \"topology-[domain].yaml\"" + System.lineSeparator();

    public static Bootstrap init(final ConfigurableEnvironment environment) {
        return new Bootstrap(environment);
    }

    private final ConfigurableEnvironment environment;

    private Bootstrap(final ConfigurableEnvironment environment) {
        this.environment = environment;
    }

    public Bootstrap validateArguments() {
        final List<String> commandLineArgs = environment.getPropertySources().stream()
                .filter(propertySource -> Objects.equals("commandLineArgs", propertySource.getName()))
                .map(propertySource -> ((CommandLinePropertySource<?>) propertySource).getPropertyNames())
                .flatMap(Arrays::stream)
                .collect(Collectors.toUnmodifiableList());
        final Boolean result = commandLineArgs.stream()
                .map(flag -> CommandLineArgument.isValid(flag, commandLineArgs))
                .reduce(true, (previous, current) -> previous && current);
        if (!result) {
            System.out.println(HELP);
            System.exit(1);
        }

        return this;
    }

    public Bootstrap handleArgumentHelp() {
        if (environment.containsProperty("help")) {
            System.out.println("Manage your Apache® Kafka Cluster including the Confluent Schema Registry.");
            System.out.println(HELP);
            System.exit(0);
        }
        return this;
    }

    public ClusterPropertiesHandler handleArgumentConfig() {
        return new ClusterPropertiesHandler(environment.getProperty("config"));
    }

}
