package de.volkerfaas.kafka.topology.bootstrap;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.DefaultApplicationArguments;

import static de.volkerfaas.kafka.topology.KafkaClusterManagerCommandLineProperty.HELP;

public class Help {

    public static final String TEXT_HELP = System.lineSeparator() +
            "Usage:" + System.lineSeparator() +
            "  java -jar kafka-cluster-manager.jar [command] [flags]" + System.lineSeparator() +
            System.lineSeparator() +
            "Available commands:" + System.lineSeparator() +
            "  create                     Create a new domain incl. service account and API keys." + System.lineSeparator() +
            "  deploy                     Deploy entire topology to cluster." + System.lineSeparator() +
            "  restore                    Restores the domains listed with flag --domain into file \"topology-[domain].yaml\"" + System.lineSeparator() +
            System.lineSeparator() +
            "Available flags for command create:" + System.lineSeparator() +
            "  --directory=[directory]    Set base directory for topology files. Default is \"topology\"." + System.lineSeparator() +
            "  --domain=[domain]          Domain to be created" + System.lineSeparator() +
            "  --description=[text]       Description of what the domain stands for" + System.lineSeparator() +
            "  --maintainer-name=[name]   Name of maintainer for given domain" + System.lineSeparator() +
            "  --maintainer-email=[name]  E-Mail of maintainer for given domain" + System.lineSeparator() +
            "  --dry-run                  Makes no changes to the local topology" + System.lineSeparator() +
            "Available flags for command deploy:" + System.lineSeparator() +
            "  --cluster=[cluster]        Sets the cluster and uses conf/[cluster].yaml or environment variables as configuration" + System.lineSeparator() +
            "  --domain=[domain]          Processes only a single domain" + System.lineSeparator() +
            "  --allow-delete-acl         Allow deletion of orphaned ACLs. Cannot be used in combination with flag --domain. (DO NOT USE IN PRODUCTION!)" + System.lineSeparator() +
            "  --allow-delete-topics      Allow deletion of orphaned topics. Cannot be used in combination with flag --domain. (DO NOT USE IN PRODUCTION!)" + System.lineSeparator() +
            "  --allow-delete-subjects    Allow deletion of orphaned subjects. Cannot be used in combination with flag --domain. (DO NOT USE IN PRODUCTION!)" + System.lineSeparator() +
            "  --dry-run                  Makes no changes to the remote topology" + System.lineSeparator() +
            "Available flags for command restore:" + System.lineSeparator() +
            "  --cluster=[cluster]        Sets the cluster and uses conf/[cluster].yaml or environment variables as configuration" + System.lineSeparator() +
            "  --domain=[domain]          Processes only a single domain" + System.lineSeparator() +
            "  --dry-run                  Makes no changes to the local topology" + System.lineSeparator() +
            System.lineSeparator() +
            "  --help                     Show help." + System.lineSeparator() +
            System.lineSeparator() +
            "Examples:" + System.lineSeparator() +
            "  java -jar kafka-cluster-manager.jar deploy" + System.lineSeparator() +
            "    → Uploads entire topology to a local cluster." + System.lineSeparator() +
            "    → Expects bootstrap server to be 'localhost:9092'" + System.lineSeparator() +
            "    → Expects schema registry url to be 'http://localhost:8081'" + System.lineSeparator() +
            "  java -jar kafka-cluster-manager.jar deploy --cluster=[cluster] " + System.lineSeparator() +
            "    → Uploads entire topology to the cluster [cluster]." + System.lineSeparator() +
            "  java -jar kafka-cluster-manager.jar deploy --cluster=[cluster] --domain=[domain]" + System.lineSeparator() +
            "    → Uploads only domain [domain] of topology to the cluster [cluster]." + System.lineSeparator() +
            "  java -jar kafka-cluster-manager.jar deploy --cluster=[cluster] --dry-run" + System.lineSeparator() +
            "    → Executes without making changes to the cluster [cluster]." + System.lineSeparator() +
            "  java -jar kafka-cluster-manager.jar restore --cluster=[cluster] --domain[domain]" + System.lineSeparator() +
            "    → Downloads domain [domain] from the cluster [cluster] to a local topology file." + System.lineSeparator() +
            "  java -jar kafka-cluster-manager.jar restore --cluster=[cluster] --domain[domain] --dry-run" + System.lineSeparator() +
            "    → Executes without making changes to the cluster [cluster]." + System.lineSeparator();

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
