package de.volkerfaas.kafka.topology;

import de.volkerfaas.kafka.topology.model.TopologyFile;
import de.volkerfaas.kafka.topology.services.DocumentationService;
import de.volkerfaas.kafka.topology.services.TopologyFileService;
import de.volkerfaas.kafka.topology.utils.ApplicationArgumentsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Component
public class KafkaClusterManager implements ApplicationRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClusterManager.class);

    private final ConfigurableEnvironment environment;
    private final TopologyFileService topologyFileService;
    private final DocumentationService documentationService;

    @Autowired
    public KafkaClusterManager(ConfigurableEnvironment environment, TopologyFileService topologyFileService, DocumentationService documentationService) {
        this.environment = environment;
        this.topologyFileService = topologyFileService;
        this.documentationService = documentationService;
    }

    @Override
    public void run(ApplicationArguments args) {
        try {
            final String cluster = ApplicationArgumentsUtils.firstStringValueOf(args, "cluster", null);
            if (Objects.isNull(cluster)) {
                LOGGER.error("No cluster set for execution.");
                return;
            }
            LOGGER.info("Using cluster '{}'.", cluster);

            final Properties properties = getProperties(cluster);
            final String directory = ApplicationArgumentsUtils.firstStringValueOf(args, "directory", "topology");
            setTopologyDirectory(properties, directory);

            final List<String> domainNames = ApplicationArgumentsUtils.stringValuesOf(args, "domain");
            final boolean allowDeleteAcl = args.containsOption("allow-delete-acl");
            final boolean allowDeleteTopics = args.containsOption("allow-delete-topics");
            final boolean dryRun = args.containsOption("dry-run");
            properties.put(ApplicationConfiguration.PROPERTY_KEY_TOPOLOGY_DRY_RUN, dryRun);

            final PropertiesPropertySource propertySource = new PropertiesPropertySource("confluent-cloud-topology-builder", properties);
            this.environment.getPropertySources().addFirst(propertySource);

            final boolean restore = args.containsOption("restore");
            if (restore) {
                restoreTopology(directory, domainNames);
            } else {
                buildTopology(directory, cluster, domainNames, allowDeleteAcl, allowDeleteTopics);
            }
        } catch (Exception e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.error("", e);
            } else {
                LOGGER.error("{}", e.getMessage());
            }
        }
    }

    public void setTopologyDirectory(Properties properties, String directory) {
        final File path = new File(directory).getAbsoluteFile();
        if (!path.exists() || !path.isDirectory()) {
            throw new IllegalStateException("Directory '" + directory + "' doesn't exist.");
        }
        final String absolutePath = path.getAbsolutePath();
        properties.put(ApplicationConfiguration.PROPERTY_KEY_TOPOLOGY_DIRECTORY, absolutePath);
        System.setProperty(ApplicationConfiguration.PROPERTY_KEY_TOPOLOGY_DIRECTORY, absolutePath);
    }

    public Properties getProperties(String cluster) throws IOException {
        final Properties properties = new Properties();
        if (Objects.nonNull(cluster)) {
            try (InputStream input = new FileInputStream(cluster + ".cluster")) {
                LOGGER.info("Configuration file found for environment '{}'", cluster);
                properties.load(input);
            } catch (FileNotFoundException e) {
                LOGGER.warn("No configuration file found for environment '{}'", cluster);
            }
        }
        return properties;
    }

    public void buildTopology(String directory, String cluster, List<String> domainNames, boolean allowDeleteAcl, boolean allowDeleteTopics) throws InterruptedException, ExecutionException, IOException {
        final Collection<TopologyFile> topologies = topologyFileService.listTopologies(directory, domainNames);
        LOGGER.debug("Topologies have been read from files: {}", topologies);
        if (topologyFileService.isTopologyValid(topologies)) {
            topologyFileService.skipTopicsNotInEnvironment(topologies, cluster);
            topologyFileService.updateTopology(topologies);
            documentationService.writeReadmeFile(topologies);
            if (allowDeleteAcl && domainNames.isEmpty()) {
                topologyFileService.deleteOrphanedAclBindings(topologies);
            }
            if (allowDeleteTopics && domainNames.isEmpty()) {
                topologyFileService.deleteOrphanedTopics(topologies);
            }
        }
    }

    public void restoreTopology(String directory, List<String> domainNames) throws ExecutionException, InterruptedException {
        if (!domainNames.isEmpty()) {
            topologyFileService.restoreTopologies(directory, domainNames);
        } else {
            LOGGER.warn("No domains to restore. Please specify domains to be restored by using the --domain=[domain] flag.");
        }
    }

}
