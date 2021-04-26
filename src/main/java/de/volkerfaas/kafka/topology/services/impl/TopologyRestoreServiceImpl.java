package de.volkerfaas.kafka.topology.services.impl;

import de.volkerfaas.kafka.cluster.model.TopicConfiguration;
import de.volkerfaas.kafka.topology.ApplicationConfiguration;
import de.volkerfaas.kafka.topology.model.*;
import de.volkerfaas.kafka.topology.repositories.TopologyFileRepository;
import de.volkerfaas.kafka.topology.services.*;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.common.acl.AclBinding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static de.volkerfaas.utils.ExceptionUtils.handleException;

@Service
public class TopologyRestoreServiceImpl implements TopologyRestoreService {

    private static final Pattern PATTERN_FULL_TOPIC_NAME = Pattern.compile(ApplicationConfiguration.REGEX_FULL_TOPIC_NAME);
    private static final Logger LOGGER = LoggerFactory.getLogger(TopologyRestoreServiceImpl.class);

    private final AccessControlService accessControlService;
    private final SchemaFileService schemaFileService;
    private final TopicService topicService;
    private final TopologyFileRepository topologyFileRepository;
    private final TopologyValuesService topologyValuesService;

    @Autowired
    public TopologyRestoreServiceImpl(AccessControlService accessControlService, SchemaFileService schemaFileService, TopicService topicService, TopologyFileRepository topologyFileRepository, TopologyValuesService topologyValuesService) {
        this.accessControlService = accessControlService;
        this.schemaFileService = schemaFileService;
        this.topicService = topicService;
        this.topologyFileRepository = topologyFileRepository;
        this.topologyValuesService = topologyValuesService;
    }

    @Override
    public Set<TopologyFile> restoreTopologies(final String directory, final List<String> domainNames) throws ExecutionException, InterruptedException, IOException, RestClientException {
        final Set<TopologyFile> topologies = listTopologiesToBeRestored(directory, domainNames);
        final Set<Schema> schemas = topologies.stream()
                .peek(topologyValuesService::addAdditionalValues)
                .peek(topologyFileRepository::writeTopology)
                .map(TopologyFile::getDomain)
                .map(Domain::getVisibilities)
                .flatMap(List::stream)
                .map(Visibility::getTopics)
                .flatMap(List::stream)
                .map(this::listTopicSchemas)
                .flatMap(Set::stream)
                .filter(Objects::nonNull)
                .collect(Collectors.toUnmodifiableSet());
        schemaFileService.downloadSchemas(schemas, directory);

        return topologies;
    }

    public TopologyFile addToTopology(final TopicConfiguration topicConfiguration, final String pathname, final List<String> domainNames, final Set<TopologyFile> topologies, final Collection<String> subjects, final Collection<AclBinding> aclBindings) throws IOException, RestClientException {
        final Matcher matcher = PATTERN_FULL_TOPIC_NAME.matcher(topicConfiguration.getName());
        if (!matcher.matches() || matcher.groupCount() != 5 || !domainNames.contains(matcher.group(1))) {
            return null;
        }
        final String domainName = matcher.group(1);
        final String visibilityType = matcher.group(2);
        final String topicName = matcher.group(3);
        final int version = Objects.nonNull(matcher.group(5)) ? Integer.parseInt(matcher.group(5)) : 0;
        final String domainResourceName = domainName + ".";
        final String visibilityResourceName = domainResourceName + visibilityType + ".";
        final String topicResourceName = visibilityResourceName + topicName;
        final String domainPrincipal = accessControlService.findPrincipalByResourceName(aclBindings, domainResourceName);
        final Collection<String> visibilityPrincipals = accessControlService.findPrincipalsByResourceName(aclBindings, visibilityResourceName);
        final Collection<String> topicPrincipals = accessControlService.findPrincipalsByResourceName(aclBindings, topicResourceName);
        final TopologyFile topology = createTopologyIfNotExists(pathname, domainName, topologies);
        final Domain domain = createDomainIfNotExistsInTopology(domainName, domainPrincipal, topology);
        final Visibility visibility = createVisibilityIfNotExistsInDomain(visibilityType, visibilityPrincipals, domain);
        final Topic topic = topicService.createTopic(topicName, topicPrincipals, topicConfiguration);
        topic.setKeySchema(schemaFileService.findSchema(subjects, domainName, topicConfiguration.getName(), "-key"));
        topic.setValueSchema(schemaFileService.findSchema(subjects, domainName, topicConfiguration.getName(), "-value"));
        topic.setVersion(version);
        visibility.getTopics().add(topic);

        return topology;
    }

    public Domain createDomainIfNotExistsInTopology(final String name, final String principal, final TopologyFile topology) {
        if (Objects.isNull(topology.getDomain())) {
            topology.setDomain(new Domain(name, principal));
        }

        return topology.getDomain();
    }

    public Visibility createVisibilityIfNotExistsInDomain(final String visibilityType, final Collection<String> principals, final Domain domain) {
        final Visibility.Type type = Visibility.Type.valueOf(visibilityType.toUpperCase());
        final List<Visibility> visibilities = domain.getVisibilities();
        final Visibility visibility = visibilities.stream()
                .filter(v -> Objects.equals(type, v.getType()))
                .findFirst()
                .orElse(createVisibility(type, principals));
        if (!visibilities.contains(visibility)) {
            visibilities.add(visibility);
        }

        return visibility;
    }

    public Visibility createVisibility(final Visibility.Type type, final Collection<String> principals) {
        final Visibility visibility = new Visibility(type);
        final List<AccessControl> consumers = principals.stream()
                .map(AccessControl::new)
                .collect(Collectors.toUnmodifiableList());
        visibility.getConsumers().addAll(consumers);

        return visibility;
    }

    public TopologyFile createTopologyIfNotExists(final String pathname, final String domainName, final Set<TopologyFile> topologies) {
        final TopologyFile topology = topologies.stream()
                .filter(t -> isTopologyFile(domainName, t))
                .findFirst()
                .orElse(createTopology(pathname, domainName));
        topologies.add(topology);

        return topology;
    }

    public TopologyFile createTopology(final String pathname, final String domainName) {
        final TopologyFile topology = new TopologyFile();
        final File file = new File(pathname, "restore-" + domainName + ".yaml");
        topology.setFile(file);

        return topology;
    }

    public boolean isTopologyFile(final String domainName, final TopologyFile topology) {
        final File file = topology.getFile();
        if (Objects.isNull(file)) {
            return false;
        }
        final String fileName = file.getName();

        return Objects.equals(fileName, "restore-" + domainName + ".yaml");
    }

    public Set<Schema> listTopicSchemas(Topic topic) {
        return Stream.of(topic.getKeySchema(), topic.getValueSchema())
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    public Set<TopologyFile> listTopologiesToBeRestored(final String pathname, final List<String> domainNames) throws ExecutionException, InterruptedException, IOException, RestClientException {
        final Set<TopologyFile> topologies = new HashSet<>();
        final Collection<TopicConfiguration> topicConfigurations = topicService.listTopicsInCluster();
        final Collection<AclBinding> aclBindings = accessControlService.listAclBindingsInCluster();
        final Collection<String> subjects = schemaFileService.listSubjects();
        if (Objects.isNull(topicConfigurations) || topicConfigurations.isEmpty()) {
            LOGGER.info("No topics to restore from cluster.");
            return Collections.emptySet();
        }
        if (Objects.isNull(subjects) || subjects.isEmpty()) {
            LOGGER.info("No schemas to restore from cluster.");
            return Collections.emptySet();
        }

        topicConfigurations.forEach(topicConfiguration -> handleException(() -> addToTopology(topicConfiguration, pathname, domainNames, topologies, subjects, aclBindings)));

        return Collections.unmodifiableSet(topologies);
    }

}
