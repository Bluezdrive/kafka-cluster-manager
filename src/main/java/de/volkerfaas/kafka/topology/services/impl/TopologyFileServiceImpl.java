package de.volkerfaas.kafka.topology.services.impl;

import de.volkerfaas.kafka.cluster.model.TopicConfiguration;
import de.volkerfaas.kafka.topology.ApplicationConfiguration;
import de.volkerfaas.kafka.topology.model.*;
import de.volkerfaas.kafka.topology.repositories.TopologyFileRepository;
import de.volkerfaas.kafka.topology.services.AccessControlService;
import de.volkerfaas.kafka.topology.services.SchemaFileService;
import de.volkerfaas.kafka.topology.services.TopicService;
import de.volkerfaas.kafka.topology.services.TopologyFileService;
import de.volkerfaas.kafka.topology.validation.ValidatorPayload;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.logging.log4j.util.Strings;
import org.hibernate.validator.HibernateValidatorFactory;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import java.io.File;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class TopologyFileServiceImpl implements TopologyFileService {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopologyFileServiceImpl.class);

    private final TopicService topicService;
    private final AccessControlService accessControlService;
    private final SchemaFileService schemaFileService;
    private final TopologyFileRepository topologyFileRepository;
    private final Validator validator;

    @Autowired
    public TopologyFileServiceImpl(TopicService topicService, AccessControlService accessControlService, SchemaFileService schemaFileService, TopologyFileRepository topologyFileRepository, Validator validator) {
        this.topicService = topicService;
        this.accessControlService = accessControlService;
        this.schemaFileService = schemaFileService;
        this.topologyFileRepository = topologyFileRepository;
        this.validator = validator;
    }

    @Override
    public Set<TopologyFile> listTopologies(String directory, List<String> domainNames) {
        return topologyFileRepository.listTopologyFiles(directory, domainNames).stream()
                .map(topologyFileRepository::readTopology)
                .filter(Objects::nonNull)
                .map(this::addAdditionalValues)
                .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public Set<TopologyFile> restoreTopologies(String directory, List<String> domainNames) throws ExecutionException, InterruptedException {
        final Set<TopologyFile> topologies = listTopologiesToBeRestored(directory, domainNames);
        topologies.stream()
                .peek(this::addAdditionalValues)
                .peek(topologyFileRepository::writeTopology)
                .map(TopologyFile::getDomain)
                .map(Domain::getVisibilities)
                .flatMap(List::stream)
                .map(Visibility::getTopics)
                .flatMap(List::stream)
                .map(Topic::getSchemaFiles)
                .flatMap(Set::stream)
                .forEach(schemaFileService::downloadSchemaFile);

        LOGGER.debug("Topology files {}", topologies);

        return topologies;
    }

    @Override
    public void updateTopology(Collection<TopologyFile> topologies) throws ExecutionException, InterruptedException {
        final List<Domain> domains = listDomains(topologies);
        if (!domains.isEmpty()) {
            final Collection<NewTopic> newTopics = topicService.createNewTopics(domains);
            final Collection<AclBinding> newAclBindings = accessControlService.listNewAclBindings(domains);
            final Map<String, NewPartitions> newPartitions = topicService.createNewPartitions(domains);
            final Map<ConfigResource, Collection<AlterConfigOp>> configs = topicService.createAlterConfigOperations(domains);
            final Collection<Path> schemaFiles = schemaFileService.listSchemaFilesByDomains(domains);
            topicService.createTopics(newTopics);
            accessControlService.createAccessControlLists(newAclBindings);
            topicService.createPartitions(newPartitions);
            topicService.updateConfigs(configs);
            schemaFileService.registerSchemaFiles(schemaFiles);
        }
    }

    @Override
    public void deleteOrphanedAclBindings(Collection<TopologyFile> topologies) throws ExecutionException, InterruptedException {
        final List<Domain> domains = listDomains(topologies);
        final Collection<AclBindingFilter> orphanedAclBindings = accessControlService.listOrphanedAclBindings(domains);
        accessControlService.deleteAccessControlLists(orphanedAclBindings);
    }

    @Override
    public void deleteOrphanedTopics(Collection<TopologyFile> topologies) throws ExecutionException, InterruptedException {
        final List<Domain> domains = listDomains(topologies);
        final Collection<String> orphanedTopics = topicService.listOrphanedTopics(domains);
        topicService.deleteTopics(orphanedTopics);
    }

    @Override
    public boolean isTopologyValid(Collection<TopologyFile> topologies) throws ExecutionException, InterruptedException {
        final Collection<TopicConfiguration> topicConfigurations = topicService.listTopicsInCluster();
        final ValidatorPayload validatorPayload = new ValidatorPayload(topologies, topicConfigurations);
        final HibernateValidatorFactory validatorFactory = this.validator.unwrap(HibernateValidatorFactory.class);
        final Validator hibernateValidator = validatorFactory.usingContext().constraintValidatorPayload(validatorPayload).getValidator();
        final Set<ConstraintViolation<TopologyFile>> violations = topologies.stream()
                .map((Function<TopologyFile, Set<ConstraintViolation<TopologyFile>>>) hibernateValidator::validate)
                .flatMap(Set::stream)
                .collect(Collectors.toUnmodifiableSet());
        if (violations.isEmpty()) {
            LOGGER.info("Topologies have been validated without constraints");
            return true;
        } else {
            violations.forEach(violation -> LOGGER.error("{} {}", violation.getPropertyPath(), violation.getMessage()));
            return false;
        }
    }

    public TopologyFile addAdditionalValues(TopologyFile topology) {
        getDomainNameAndVisibilities(topology.getDomain()).stream()
                .map(this::addAdditionalValuesToVisibility)
                .map(this::getDomainNameAndVisibilityFullNameAndTopics)
                .flatMap(List::stream)
                .forEach(this::addAdditionalValuesToTopic);

        return topology;
    }

    public Pair<String, Visibility> addAdditionalValuesToVisibility(Pair<String, Visibility> pair) {
        final String domainName = pair.getValue0();
        final Visibility visibility = pair.getValue1();
        visibility.setPrefix(domainName + ".");

        return pair;
    }

    public void addAdditionalValuesToTopic(Triplet<String, String, Topic> triplet) {
        final String domainName = triplet.getValue0();
        final String visibilityFullName = triplet.getValue1();
        final Topic topic = triplet.getValue2();
        topic.setPrefix(visibilityFullName + ".");
        if (Strings.isEmpty(topic.getValueSchemaFile())) {
            final String valueSchemaFile = getValueSchemaFilename(domainName, topic);
            topic.setValueSchemaFile(valueSchemaFile);
        }
    }

    public void addToTopology(String pathname, List<String> domainNames, Set<TopologyFile> topologies, Collection<String> subjects, TopicConfiguration topicConfiguration, Collection<AclBinding> aclBindings) {
        final Pattern pattern = Pattern.compile(ApplicationConfiguration.REGEX_FULL_TOPIC_NAME);
        final Matcher matcher = pattern.matcher(topicConfiguration.getName());
        if (matcher.matches() && matcher.groupCount() == 3 && domainNames.contains(matcher.group(1))) {
            final String domainName = matcher.group(1);
            final String visibilityType = matcher.group(2);
            final String topicName = matcher.group(3);
            final String domainResourceName = domainName + ".";
            final String visibilityResourceName = domainResourceName + visibilityType + ".";
            final String topicResourceName = domainResourceName + visibilityType + "." + topicName;
            final String domainPrincipal = accessControlService.findPrincipalByResourceName(aclBindings, domainResourceName);
            final Collection<String> visibilityPrincipals = accessControlService.findPrincipalsByResourceName(aclBindings, visibilityResourceName);
            final Collection<String> topicPrincipals = accessControlService.findPrincipalsByResourceName(aclBindings, topicResourceName);
            final String keySchemaFile = schemaFileService.findSchema(subjects, domainName, topicConfiguration.getName(), "-key");
            final String valueSchemaFile = schemaFileService.findSchema(subjects, domainName, topicConfiguration.getName(), "-value");
            final TopologyFile topology = createTopologyIfNotExists(pathname, domainName, topologies);
            final Domain domain = createDomainIfNotExistsInTopology(domainName, domainPrincipal, topology);
            final Visibility visibility = createVisibilityIfNotExistsInDomain(visibilityType, visibilityPrincipals, domain);
            final Topic topic = topicService.createTopic(topicName, topicPrincipals, topicConfiguration);
            topic.setKeySchemaFile(keySchemaFile);
            topic.setValueSchemaFile(valueSchemaFile);
            visibility.getTopics().add(topic);
        }
    }

    public TopologyFile createTopologyIfNotExists(final String pathname, final String domainName, final Set<TopologyFile> topologies) {
        TopologyFile topology = topologies.stream()
                .filter(t -> isTopologyFileMatch(domainName, t))
                .findFirst()
                .orElse(null);
        if (Objects.isNull(topology)) {
            topology = new TopologyFile();
            topology.setFile(new File(pathname, "restore-" + domainName + ".yaml"));
            topologies.add(topology);
        }

        return topology;
    }

    public boolean isTopologyFileMatch(String domainName, TopologyFile topology) {
        final File file = topology.getFile();
        if (Objects.isNull(file)) {
            return false;
        }

        return Objects.equals(file.getName(), "topology-" + domainName + ".yaml");
    }

    public Domain createDomainIfNotExistsInTopology(final String name, String principal, final TopologyFile topology) {
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

    public String getValueSchemaFilename(String domainName, Topic topic) {
        return ApplicationConfiguration.EVENTS_DIRECTORY
                + "/" + domainName
                + "/" + topic.getFullName()
                + ApplicationConfiguration.SCHEMA_FILE_VALUE_SUFFIX;
    }

    public List<Triplet<String, String, Topic>> getDomainNameAndVisibilityFullNameAndTopics(Pair<String, Visibility> pair) {
        return pair.getValue1().getTopics().stream()
                .map(topic -> Triplet.with(pair.getValue0(), pair.getValue1().getFullName(), topic))
                .collect(Collectors.toUnmodifiableList());
    }

    public List<Pair<String, Visibility>> getDomainNameAndVisibilities(Domain domain) {
        return domain.getVisibilities().stream()
                .map(visibility -> Pair.with(domain.getName(), visibility))
                .collect(Collectors.toUnmodifiableList());
    }

    public List<Domain> listDomains(Collection<TopologyFile> topologies) {
        return topologies.stream()
                .map(TopologyFile::getDomain)
                .collect(Collectors.toUnmodifiableList());
    }

    public Set<TopologyFile> listTopologiesToBeRestored(String pathname, List<String> domainNames) throws ExecutionException, InterruptedException {
        final Set<TopologyFile> topologies = new HashSet<>();
        final Collection<TopicConfiguration> topicConfigurations = topicService.listTopicsInCluster();
        final Collection<AclBinding> aclBindings = accessControlService.listAclBindingsInCluster();
        final Collection<String> subjects = schemaFileService.listSubjects();
        if (Objects.isNull(topicConfigurations) || topicConfigurations.isEmpty() || Objects.isNull(subjects) || subjects.isEmpty()) {
            LOGGER.info("No topics to restore from cluster.");
            return Collections.emptySet();
        }
        topicConfigurations.forEach(topicDescription -> addToTopology(pathname, domainNames, topologies, subjects, topicDescription, aclBindings));

        return Collections.unmodifiableSet(topologies);
    }

}
