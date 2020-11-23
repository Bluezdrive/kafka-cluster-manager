package de.volkerfaas.kafka.topology.services.impl;

import com.github.freva.asciitable.AsciiTable;
import com.github.freva.asciitable.Column;
import de.volkerfaas.kafka.cluster.model.TopicConfiguration;
import de.volkerfaas.kafka.topology.ApplicationConfiguration;
import de.volkerfaas.kafka.topology.model.*;
import de.volkerfaas.kafka.topology.repositories.TopologyFileRepository;
import de.volkerfaas.kafka.topology.services.*;
import org.apache.kafka.common.acl.AclBinding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.github.freva.asciitable.HorizontalAlign.LEFT;

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
    public Set<TopologyFile> restoreTopologies(final String directory, final List<String> domainNames) throws ExecutionException, InterruptedException {
        final Set<TopologyFile> topologies = listTopologiesToBeRestored(directory, domainNames);
        final Set<Topic> topics = topologies.stream()
                .peek(topologyValuesService::addAdditionalValues)
                .peek(topologyFileRepository::writeTopology)
                .map(TopologyFile::getDomain)
                .map(Domain::getVisibilities)
                .flatMap(List::stream)
                .map(Visibility::getTopics)
                .flatMap(List::stream)
                .peek(topic -> schemaFileService.downloadSchemas(topic.getSchemas(), directory))
                .collect(Collectors.toUnmodifiableSet());
        printTopics(topics);

        return topologies;
    }

    public void addToTopology(final TopicConfiguration topicConfiguration, final String pathname, final List<String> domainNames, final Set<TopologyFile> topologies, final Collection<String> subjects, final Collection<AclBinding> aclBindings) {
        final Matcher matcher = PATTERN_FULL_TOPIC_NAME.matcher(topicConfiguration.getName());
        if (!matcher.matches() || matcher.groupCount() != 3 || !domainNames.contains(matcher.group(1))) {
            return;
        }
        final String domainName = matcher.group(1);
        final String visibilityType = matcher.group(2);
        final String topicName = matcher.group(3);
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
        visibility.getTopics().add(topic);
    }

    public Domain createDomainIfNotExistsInTopology(final String name, final String principal, final TopologyFile topology) {
        Domain domain = topology.getDomain();
        if (Objects.isNull(domain)) {
            domain = new Domain(name, principal);
            topology.setDomain(domain);
        }

        return domain;
    }

    public Visibility createVisibilityIfNotExistsInDomain(final String visibilityType, final Collection<String> principals, final Domain domain) {
        final Visibility.Type type = Visibility.Type.valueOf(visibilityType.toUpperCase());
        Visibility visibility = domain.getVisibilities().stream()
                .filter(v -> Objects.equals(type, v.getType()))
                .findFirst()
                .orElse(null);
        if (Objects.isNull(visibility)) {
            visibility = new Visibility();
            visibility.setType(type);
            visibility.getConsumers().addAll(principals.stream().map(AccessControl::new).collect(Collectors.toUnmodifiableList()));
            domain.getVisibilities().add(visibility);
        }

        return visibility;
    }

    public TopologyFile createTopologyIfNotExists(final String pathname, final String domainName, final Set<TopologyFile> topologies) {
        TopologyFile topology = topologies.stream()
                .filter(t -> isTopologyFile(domainName, t))
                .findFirst()
                .orElse(null);
        if (Objects.isNull(topology)) {
            topology = new TopologyFile();
            topology.setFile(new File(pathname, "restore-" + domainName + ".yaml"));
            topologies.add(topology);
        }

        return topology;
    }

    public boolean isTopologyFile(final String domainName, final TopologyFile topology) {
        final File file = topology.getFile();
        if (Objects.isNull(file)) {
            return false;
        }

        return Objects.equals(file.getName(), "topology-" + domainName + ".yaml");
    }

    public Set<TopologyFile> listTopologiesToBeRestored(final String pathname, final List<String> domainNames) throws ExecutionException, InterruptedException {
        final Set<TopologyFile> topologies = new HashSet<>();
        final Collection<TopicConfiguration> topicConfigurations = topicService.listTopicsInCluster();
        final Collection<AclBinding> aclBindings = accessControlService.listAclBindingsInCluster();
        final Collection<String> subjects = schemaFileService.listSubjects();
        if (Objects.isNull(topicConfigurations) || topicConfigurations.isEmpty() || Objects.isNull(subjects) || subjects.isEmpty()) {
            LOGGER.info("No topics to restore from cluster.");
            return Collections.emptySet();
        }

        topicConfigurations.forEach(topicConfiguration -> addToTopology(topicConfiguration, pathname, domainNames, topologies, subjects, aclBindings));

        return Collections.unmodifiableSet(topologies);
    }

    private void printTopics(final Set<Topic> topics) {
        System.out.println(AsciiTable.getTable(topics, Arrays.asList(
                new Column().header("Topic").dataAlign(LEFT).with(Topic::getFullName),
                new Column().header("Partitions").dataAlign(LEFT).with(topic -> String.valueOf(topic.getNumPartitions())),
                new Column().header("Replication Factor").dataAlign(LEFT).with(topic -> String.valueOf(topic.getReplicationFactor())),
                new Column().header("Key Schema").dataAlign(LEFT).with(topic -> {
                    final Schema keySchema = topic.getKeySchema();
                    if (Objects.isNull(keySchema)) return null;
                    return keySchema.getSubject();
                }),
                new Column().header("Key Schema Type").dataAlign(LEFT).with(topic -> {
                    final Schema keySchema = topic.getKeySchema();
                    if (Objects.isNull(keySchema) || Objects.isNull(keySchema.getType())) return null;
                    return keySchema.getType().toString();
                }),
                new Column().header("Value Schema").dataAlign(LEFT).with(topic -> {
                    final Schema valueSchema = topic.getValueSchema();
                    if (Objects.isNull(valueSchema)) return null;
                    return valueSchema.getSubject();
                }),
                new Column().header("Value Schema Type").dataAlign(LEFT).with(topic -> {
                    final Schema valueSchema = topic.getValueSchema();
                    if (Objects.isNull(valueSchema) || Objects.isNull(valueSchema.getType())) return null;
                    return valueSchema.getType().toString();
                })

        )));
    }

}
