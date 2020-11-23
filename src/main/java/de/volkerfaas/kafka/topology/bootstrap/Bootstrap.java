package de.volkerfaas.kafka.topology.bootstrap;

import org.springframework.core.env.Environment;

public final class Bootstrap {

    public static final String HELP = System.lineSeparator() +
            "Usage:" + System.lineSeparator() +
            "  java -jar kafka-cluster-manager.jar [flags]" + System.lineSeparator() +
            System.lineSeparator() +
            "Available Flags:" + System.lineSeparator() +
            "  --help                     Show help." + System.lineSeparator() +
            "  --directory=[directory]    Set base directory for topology files. Default is \"topology\"." + System.lineSeparator() +
            "  --domain=[domain]          Processes only a single domain" + System.lineSeparator() +
            "  --config=[config]          Sets the configuration file (without .cluster) to use to connect to the cluster" + System.lineSeparator() +
            "  --allow-delete-acl         Allow deletion of orphaned ACLs. Cannot be used in combination with flag --domain. (DO NOT USE IN PRODUCTION!)" + System.lineSeparator() +
            "  --allow-delete-topics      Allow deletion of orphaned topics. Cannot be used in combination with flag --domain. (DO NOT USE IN PRODUCTION!)" + System.lineSeparator() +
            "  --dry-run                  Makes no changes to the remote topology" + System.lineSeparator() +
            "  --restore                  Restores the domains listed with flag --domain into file \"topology-[domain].yaml\"" + System.lineSeparator();

    public static Bootstrap init(Environment environment) {
        return new Bootstrap(environment);
    }

    private final Environment environment;

    private Bootstrap(Environment environment) {
        this.environment = environment;
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
        if (environment.containsProperty("config")) {
            final String cluster = environment.getProperty("config");
            return new ClusterPropertiesHandler(cluster);
        } else {
            return new ClusterPropertiesHandler();
        }
    }

}
