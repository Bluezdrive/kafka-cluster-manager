package de.volkerfaas.kafka.topology;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.Locale;

@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        if (Arrays.asList(args).contains("--help")) {
            System.out.println("Manage your Apache® Kafka Cluster including the Confluent Schema Registry.");
            System.out.println();
            System.out.println("Usage:");
            System.out.println("  java -jar kafka-cluster-manager.jar [flags]");
            System.out.println();
            System.out.println("Available Flags:");
            System.out.println("  --help                     Show help.");
            System.out.println("  --directory=[directory]    Set base directory for topology files. Default is \"topology\".");
            System.out.println("  --domain=[domain]          Processes only a single domain");
            System.out.println("  --allow-delete-acl         Allow deletion of orphaned ACLs. Cannot be used in combination with flag --domain. (DO NOT USE IN PRODUCTION!)");
            System.out.println("  --allow-delete-topics      Allow deletion of orphaned topics. Cannot be used in combination with flag --domain. (DO NOT USE IN PRODUCTION!)");
            System.out.println("  --dry-run                  Makes no changes to the remote topology");
            System.out.println("  --restore                  Restores the domains listed with flag --domain into file \"topology-[domain].yaml\"");
        } else {
            Locale.setDefault(Locale.ENGLISH);
            SpringApplication.run(Application.class, args);
        }
    }

}
