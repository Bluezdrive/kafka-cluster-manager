package de.volkerfaas.kafka.topology;

import de.volkerfaas.kafka.topology.model.Domain;
import de.volkerfaas.kafka.topology.model.TopologyFile;
import de.volkerfaas.kafka.topology.services.DocumentationService;
import de.volkerfaas.kafka.topology.services.TopologyBuildService;
import de.volkerfaas.kafka.topology.services.TopologyRestoreService;
import de.volkerfaas.utils.CommandLineArguments;
import de.volkerfaas.utils.DefaultCommandLineArguments;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Component
public class KafkaClusterManager implements CommandLineRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClusterManager.class);

    private final DocumentationService documentationService;
    private final TopologyBuildService topologyBuildService;
    private final TopologyRestoreService topologyRestoreService;

    @Autowired
    public KafkaClusterManager(final DocumentationService documentationService, final TopologyBuildService topologyBuildService, final TopologyRestoreService topologyRestoreService) {
        this.documentationService = documentationService;
        this.topologyBuildService = topologyBuildService;
        this.topologyRestoreService = topologyRestoreService;
    }

    @Override
    public void run(String[] args) {
        final CommandLineArguments arguments = new DefaultCommandLineArguments(args);
        final String directory = getTopologyDirectory(arguments);
        final List<String> domainNames = arguments.getOptionValues(KafkaClusterManagerCommandLineArgument.DOMAIN);
        final boolean restore = arguments.containsOption(KafkaClusterManagerCommandLineArgument.RESTORE);
        try {
            if (restore) {
                restoreTopology(directory, domainNames);
            } else {
                final boolean allowDeleteAcl = arguments.containsOption(KafkaClusterManagerCommandLineArgument.ALLOW_DELETE_ACL);
                final boolean allowDeleteTopics = arguments.containsOption(KafkaClusterManagerCommandLineArgument.ALLOW_DELETE_TOPICS);
                final String cluster = arguments.getRequiredOptionValue(KafkaClusterManagerCommandLineArgument.CLUSTER);
                buildTopology(directory, domainNames, allowDeleteAcl, allowDeleteTopics, cluster);
            }
        } catch (IllegalCommandLineArgumentException e) {
            System.exit(3);
        } catch (IllegalTopologyException e) {
            System.exit(2);
        } catch (AssertionError e) {
            throw e;
        } catch (Throwable e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.error("{}", e.getMessage(), e);
            } else {
                LOGGER.error("{}", e.getMessage());
            }
            System.exit(1);
        }
    }

    public String getTopologyDirectory(final CommandLineArguments arguments) {
        final String directory = arguments.getOptionValue(KafkaClusterManagerCommandLineArgument.DIRECTORY);
        final File path = new File(directory).getAbsoluteFile();
        if (!path.exists() || !path.isDirectory()) {
            throw new IllegalStateException("Directory '" + directory + "' doesn't exist.");
        }
        return path.getAbsolutePath();
    }

    public void buildTopology(final String directory, final Collection<String> domainNames, final boolean allowDeleteAcl, final boolean allowDeleteTopics, final String cluster) throws InterruptedException, ExecutionException, IOException, IllegalTopologyException, IllegalCommandLineArgumentException {
        final Collection<TopologyFile> topologies = topologyBuildService.listTopologies(directory);
        if (topologies.isEmpty()) {
            LOGGER.debug("No topologies to build.");
            return;
        }
        LOGGER.debug("Topologies have been read from files: {}", topologies);
        final boolean valid = topologyBuildService.isTopologyValid(topologies, directory);
        if (!valid) {
            throw new IllegalTopologyException();
        }
        topologyBuildService.removeTopicsNotInCluster(topologies, cluster);
        final Collection<Domain> domains = topologyBuildService.filterDomainsForUpdate(topologies, domainNames);
        topologyBuildService.updateTopology(domains, directory);
        documentationService.writeReadmeFile(topologies, directory);
        if ((allowDeleteAcl || allowDeleteTopics) && !domainNames.isEmpty()) {
            LOGGER.error("Usage of flag --allow-delete-acl or --allow-delete-topics only allowed without flag --domain");
            throw new IllegalCommandLineArgumentException();
        }
        if (allowDeleteAcl) {
            topologyBuildService.deleteOrphanedAclBindings(domains);
        }
        if (allowDeleteTopics) {
            topologyBuildService.deleteOrphanedTopics(domains);
        }
    }

    public void restoreTopology(final String directory, final List<String> domainNames) throws ExecutionException, InterruptedException, IOException, RestClientException {
        if (domainNames.isEmpty()) {
            LOGGER.warn("No domains to restore. Please specify domains to be restored by using the --domain=[domain] flag.");
            return;
        }
        topologyRestoreService.restoreTopologies(directory, domainNames);
    }

}
