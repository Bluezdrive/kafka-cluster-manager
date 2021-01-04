package de.volkerfaas.kafka.topology.bootstrap;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.DefaultApplicationArguments;

import static de.volkerfaas.kafka.topology.KafkaClusterManagerCommandLineArgument.HELP;

public class Help {

    public static final String TEXT_HELP = System.lineSeparator() +
            "Usage:" + System.lineSeparator() +
            "  java -jar kafka-cluster-manager.jar [flags]" + System.lineSeparator() +
            System.lineSeparator() +
            "Examples:" + System.lineSeparator() +
            "  java -jar kafka-cluster-manager.jar --cluster=[cluster] " + System.lineSeparator() +
            "    → Uploads a topology to the cluster [cluster]." + System.lineSeparator() +
            "  java -jar kafka-cluster-manager.jar --cluster=[cluster] --domain=[domain]" + System.lineSeparator() +
            "    → Uploads only domain [domain] of topology to the cluster [cluster]." + System.lineSeparator() +
            "  java -jar kafka-cluster-manager.jar --cluster=[cluster] --dry-run" + System.lineSeparator() +
            "    → Executes without making changes to the cluster [cluster]." + System.lineSeparator() +
            "  java -jar kafka-cluster-manager.jar --cluster=[cluster] --restore --domain[domain]" + System.lineSeparator() +
            "    → Downloads domain [domain] from the cluster [cluster] to a local topology file." + System.lineSeparator() +
            "  java -jar kafka-cluster-manager.jar --cluster=[cluster] --restore --domain[domain] --dry-run" + System.lineSeparator() +
            "    → Executes without making changes to the cluster [cluster]." + System.lineSeparator() +
            System.lineSeparator() +
            "Available Flags:" + System.lineSeparator() +
            "  --help                     Show help." + System.lineSeparator() +
            "  --directory=[directory]    Set base directory for topology files. Default is \"topology\"." + System.lineSeparator() +
            "  --domain=[domain]          Processes only a single domain" + System.lineSeparator() +
            "  --cluster=[cluster]        Sets the cluster and uses conf/[cluster].properties or environment variables as configuration" + System.lineSeparator() +
            "  --allow-delete-acl         Allow deletion of orphaned ACLs. Cannot be used in combination with flag --domain. (DO NOT USE IN PRODUCTION!)" + System.lineSeparator() +
            "  --allow-delete-topics      Allow deletion of orphaned topics. Cannot be used in combination with flag --domain. (DO NOT USE IN PRODUCTION!)" + System.lineSeparator() +
            "  --dry-run                  Makes no changes to the remote topology" + System.lineSeparator() +
            "  --write-documentation-file Writes the documentation file \"topology.md\" to the topology directory" + System.lineSeparator() +
            "  --restore                  Restores the domains listed with flag --domain into file \"topology-[domain].yaml\"" + System.lineSeparator();

    private final ApplicationArguments applicationArguments;
    private boolean result;

    public static Help init(final String[] args) {
        return new Help(new DefaultApplicationArguments(args));
    }

    private Help(final ApplicationArguments applicationArguments) {
        this.applicationArguments = applicationArguments;
        this.result = false;
    }

    public Help handleArgumentHelp() {
        if (applicationArguments.containsOption(HELP.getName())) {
            System.out.println("Manage your Apache® Kafka Cluster including the Confluent Schema Registry.");
            System.out.println(TEXT_HELP);
            this.result = true;
        }
        return this;
    }

    public boolean result() {
        return result;
    }

}
