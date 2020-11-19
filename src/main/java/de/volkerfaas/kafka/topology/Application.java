package de.volkerfaas.kafka.topology;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.*;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;

@SpringBootApplication
public class Application {

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";

    public static void main(String[] args) throws IOException {
        Locale.setDefault(Locale.ENGLISH);

        if (Arrays.asList(args).contains("--help")) {
            System.out.println("Manage your Apache® Kafka Cluster including the Confluent Schema Registry.");
            printHelp();

            return;
        }

        final String cluster = getCluster(args);
        if (Objects.isNull(cluster)) {
            System.out.println(ANSI_RED + "ERROR " + ANSI_RESET + "No cluster set for execution. Please set cluster by using --cluster flag.");
            printHelp();

            return;
        }
        loadProperties(cluster);

        boolean result = isSystemPropertySet("BOOTSTRAP_SERVER", false);
        result = isSystemPropertySet("CLUSTER_API_KEY", true) && result;
        result = isSystemPropertySet("CLUSTER_API_SECRET", true) && result;
        result = isSystemPropertySet("SCHEMA_REGISTRY_URL", false) && result;
        result = isSystemPropertySet("SCHEMA_REGISTRY_API_KEY", true) && result;
        result = isSystemPropertySet("SCHEMA_REGISTRY_API_SECRET", true) && result;
        if (!result) {
            return;
        }

        SpringApplication.run(Application.class, args);
    }

    public static String getCluster(String[] args) {
        return Arrays.stream(args)
                .filter(a -> a.startsWith("--cluster"))
                .map(Application::getArgumentValue)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    public static void printHelp() {
        System.out.println();
        System.out.println("Usage:");
        System.out.println("  java -jar kafka-cluster-manager.jar [flags]");
        System.out.println();
        System.out.println("Available Flags:");
        System.out.println("  --help                     Show help.");
        System.out.println("  --directory=[directory]    Set base directory for topology files. Default is \"topology\".");
        System.out.println("  --domain=[domain]          Processes only a single domain");
        System.out.println("  --cluster=[cluster]        Sets the cluster to deploy to or to restore from");
        System.out.println("  --allow-delete-acl         Allow deletion of orphaned ACLs. Cannot be used in combination with flag --domain. (DO NOT USE IN PRODUCTION!)");
        System.out.println("  --allow-delete-topics      Allow deletion of orphaned topics. Cannot be used in combination with flag --domain. (DO NOT USE IN PRODUCTION!)");
        System.out.println("  --dry-run                  Makes no changes to the remote topology");
        System.out.println("  --restore                  Restores the domains listed with flag --domain into file \"topology-[domain].yaml\"");
    }

    public static String getArgumentValue(String argument) {
        final String[] strings = argument.split("=");
        if (strings.length != 2) {
            return null;
        }

        return strings[1];
    }

    public static boolean isSystemPropertySet(String key, boolean secret) {
        String value = System.getProperty(key);
        if (Objects.isNull(value)) {
            value = System.getenv(key);
        }
        if (Objects.isNull(value) || value.isBlank()) {
            System.out.println(ANSI_RED + "ERROR " + ANSI_RESET + "Property '" + key + "' not set!");

            return false;
        }
        System.out.println(ANSI_GREEN + " INFO " + ANSI_RESET + key + "=" + (secret ? "[secret]" : value));

        return true;
    }

    public static void loadProperties(String cluster) throws IOException {
        final File file = new File("conf/", cluster + ".cluster");
        final Properties properties = new Properties();
        try (InputStream input = new FileInputStream(file)) {
            properties.load(input);
            properties.forEach((key, value) -> System.setProperty(key.toString(), value.toString()));
            System.out.println(ANSI_GREEN + " INFO " + ANSI_RESET + "Using configuration file for cluster '" + cluster + "'");
        } catch (FileNotFoundException e) {
            System.out.println(ANSI_YELLOW + " WARN " + ANSI_RESET + "No configuration file found for cluster '" + cluster + "'");
        }
    }

}
