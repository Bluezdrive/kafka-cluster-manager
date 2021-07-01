package de.volkerfaas.kafka.topology;

import com.fasterxml.jackson.core.JsonProcessingException;
import de.volkerfaas.kafka.topology.model.Domain;
import de.volkerfaas.kafka.topology.model.TopologyFile;
import de.volkerfaas.kafka.topology.services.*;
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
    private final TopologyCreateService topologyCreateService;
    private final TopologyDeleteService topologyDeleteService;
    private final TopologyDeployService topologyDeployService;
    private final TopologyRestoreService topologyRestoreService;

    @Autowired
    public KafkaClusterManager(final DocumentationService documentationService, final TopologyDeployService topologyDeployService, final TopologyCreateService topologyCreateService, final TopologyRestoreService topologyRestoreService, final TopologyDeleteService topologyDeleteService) {
        this.documentationService = documentationService;
        this.topologyCreateService = topologyCreateService;
        this.topologyDeleteService = topologyDeleteService;
        this.topologyDeployService = topologyDeployService;
        this.topologyRestoreService = topologyRestoreService;
    }

    @Override
    public void run(String[] args) {
        final CommandLineArguments arguments = new DefaultCommandLineArguments(args);
        final String directory = getTopologyDirectory(arguments);
        final boolean create = arguments.containsOption(KafkaClusterManagerCommandLineOption.CREATE);
        final boolean delete = arguments.containsOption(KafkaClusterManagerCommandLineOption.DELETE);
        final boolean deploy = arguments.containsOption(KafkaClusterManagerCommandLineOption.DEPLOY);
        final boolean restore = arguments.containsOption(KafkaClusterManagerCommandLineOption.RESTORE);
        try {
            if (create) {
                final String domainName = arguments.getPropertyValue(KafkaClusterManagerCommandLineProperty.DOMAIN);
                final String description = arguments.getPropertyValue(KafkaClusterManagerCommandLineProperty.DESCRIPTION);
                final String maintainerName = arguments.getPropertyValue(KafkaClusterManagerCommandLineProperty.MAINTAINER_NAME);
                final String maintainerEmail = arguments.getPropertyValue(KafkaClusterManagerCommandLineProperty.MAINTAINER_EMAIL);
                final String serviceAccountId = arguments.getPropertyValue(KafkaClusterManagerCommandLineProperty.SERVICE_ACCOUNT_ID);
                createTopology(directory, domainName, description, maintainerName, maintainerEmail, serviceAccountId);
            } else if (delete) {
                final List<String> domainNames = arguments.getPropertyValues(KafkaClusterManagerCommandLineProperty.DOMAIN);
                deleteTopology(directory, domainNames);
            } else if (deploy) {
                final boolean allowDeleteAcl = arguments.containsProperty(KafkaClusterManagerCommandLineProperty.ALLOW_DELETE_ACL);
                final boolean allowDeleteSubjects = arguments.containsProperty(KafkaClusterManagerCommandLineProperty.ALLOW_DELETE_SUBJECTS);
                final boolean allowDeleteTopics = arguments.containsProperty(KafkaClusterManagerCommandLineProperty.ALLOW_DELETE_TOPICS);
                final String cluster = arguments.getRequiredPropertyValue(KafkaClusterManagerCommandLineProperty.CLUSTER);
                final List<String> domainNames = arguments.getPropertyValues(KafkaClusterManagerCommandLineProperty.DOMAIN);
                deployTopology(directory, domainNames, allowDeleteAcl, allowDeleteSubjects, allowDeleteTopics, cluster);
            } else if (restore) {
                final List<String> domainNames = arguments.getPropertyValues(KafkaClusterManagerCommandLineProperty.DOMAIN);
                restoreTopology(directory, domainNames);
            } else {
                LOGGER.error("Please choose one of create, deploy or restore");
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
        final String directory = arguments.getPropertyValue(KafkaClusterManagerCommandLineProperty.DIRECTORY);
        final File path = new File(directory).getAbsoluteFile();
        if (!path.exists() || !path.isDirectory()) {
            throw new IllegalStateException("Directory '" + directory + "' doesn't exist.");
        }
        return path.getAbsolutePath();
    }

    public void createTopology(final String directory, final String domainName, final String description, final String maintainerName, final String maintainerEmail, final String serviceAccountId) throws JsonProcessingException {
        topologyCreateService.createTopology(directory, domainName, description, maintainerName, maintainerEmail, serviceAccountId);
    }

    public void deleteTopology(final String directory, final Collection<String> domainNames) {
        topologyDeleteService.deleteTopology(directory, domainNames);
    }

    public void deployTopology(final String directory, final Collection<String> domainNames, final boolean allowDeleteAcl, final boolean allowDeleteSubjects, final boolean allowDeleteTopics, final String cluster) throws InterruptedException, ExecutionException, IOException, IllegalTopologyException, IllegalCommandLineArgumentException, RestClientException {
        final Collection<TopologyFile> topologies = topologyDeployService.listTopologies(directory);
        if (topologies.isEmpty()) {
            LOGGER.debug("No topologies to build.");
            return;
        }
        LOGGER.debug("Topologies have been read from files: {}", topologies);
        final boolean valid = topologyDeployService.isTopologyValid(topologies, directory);
        if (!valid) {
            throw new IllegalTopologyException();
        }
        topologyDeployService.removeTopicsNotInCluster(topologies, cluster);
        final Collection<Domain> domains = topologyDeployService.filterDomainsForUpdate(topologies, domainNames);
        topologyDeployService.updateTopology(domains, directory);
        documentationService.writeTopologyDocumentationFile(topologies, directory);
        documentationService.writeEventsDocumentationFile(topologies, directory);
        if ((allowDeleteAcl || allowDeleteTopics) && !domainNames.isEmpty()) {
            LOGGER.error("Usage of flag --allow-delete-acl or --allow-delete-topics only allowed without flag --domain");
            throw new IllegalCommandLineArgumentException();
        }
        if (allowDeleteAcl) {
            topologyDeployService.deleteOrphanedAclBindings(domains);
        }
        if (allowDeleteTopics) {
            topologyDeployService.deleteOrphanedTopics(domains);
        }
        if (allowDeleteSubjects) {
            topologyDeployService.deleteOrphanedSubjects(domains);
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
