package de.volkerfaas.kafka.topology.services.impl;

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
import org.springframework.core.env.Environment;
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

    private final Environment environment;
    private final TopicService topicService;
    private final AccessControlService accessControlService;
    private final SchemaFileService schemaFileService;
    private final TopologyFileRepository topologyFileRepository;
    private final Validator validator;

    @Autowired
    public TopologyFileServiceImpl(Environment environment, TopicService topicService, AccessControlService accessControlService, SchemaFileService schemaFileService, TopologyFileRepository topologyFileRepository, Validator validator) {
        this.environment = environment;
        this.topicService = topicService;
        this.accessControlService = accessControlService;
        this.schemaFileService = schemaFileService;
        this.topologyFileRepository = topologyFileRepository;
        this.validator = validator;
    }

    @Override
    public Set<TopologyFile> listTopologies(String directory, List<String> domainNames) {
        final Set<TopologyFile> topologies = topologyFileRepository.listTopologyFiles(directory, domainNames).stream()
                .map(topologyFileRepository::readTopology)
                .filter(Objects::nonNull)
                .map(this::addAdditionalValues)
                .collect(Collectors.toUnmodifiableSet());
        LOGGER.debug("Topologies have been read from files: {}", topologies);

        return topologies;
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
    public void updateTopology(Set<TopologyFile> topologies) throws ExecutionException, InterruptedException {
        final List<Domain> domains = listDomains(topologies);
        if (!domains.isEmpty()) {
            final Set<NewTopic> newTopics = topicService.createNewTopics(domains);
            final Set<AclBinding> newAclBindings = accessControlService.listNewAclBindings(domains);
            final Map<String, NewPartitions> newPartitions = topicService.createNewPartitions(domains);
            final Map<ConfigResource, Collection<AlterConfigOp>> configs = topicService.createAlterConfigOperations(domains);
            final Set<Path> schemaFiles = schemaFileService.updateSchemaFiles(domains);
            topicService.createTopics(newTopics);
            accessControlService.createAccessControlLists(newAclBindings);
            topicService.createPartitions(newPartitions);
            topicService.updateConfigs(configs);
            schemaFileService.registerSchemaFiles(schemaFiles);
        }

        boolean allowDelete = environment.getRequiredProperty(ApplicationConfiguration.PROPERTY_KEY_TOPOLOGY_ALLOW_DELETE, boolean.class);
        if (allowDelete) {
            final Set<AclBindingFilter> orphanedAclBindings = accessControlService.listOrphanedAclBindings(domains);
            accessControlService.deleteAccessControlLists(orphanedAclBindings);
        }
    }

    @Override
    public boolean isTopologyValid(Set<TopologyFile> topologies) throws ExecutionException, InterruptedException {
        final List<KafkaTopic> kafkaTopics = topicService.listTopicsInCluster();
        final ValidatorPayload validatorPayload = new ValidatorPayload(topologies, kafkaTopics);
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

    TopologyFile addAdditionalValues(TopologyFile topology) {
        getDomainNameAndVisibilities(topology.getDomain()).stream()
                .map(this::addAdditionalValuesToVisibility)
                .map(this::getDomainNameAndVisibilityFullNameAndTopics)
                .flatMap(List::stream)
                .forEach(this::addAdditionalValuesToTopic);

        return topology;
    }

    Pair<String, Visibility> addAdditionalValuesToVisibility(Pair<String, Visibility> pair) {
        final String domainName = pair.getValue0();
        final Visibility visibility = pair.getValue1();
        visibility.setPrefix(domainName + ".");

        return pair;
    }

    void addAdditionalValuesToTopic(Triplet<String, String, Topic> triplet) {
        final String domainName = triplet.getValue0();
        final String visibilityFullName = triplet.getValue1();
        final Topic topic = triplet.getValue2();
        topic.setPrefix(visibilityFullName + ".");
        if (Strings.isEmpty(topic.getValueSchemaFile())) {
            final String valueSchemaFile = getValueSchemaFilename(domainName, topic);
            topic.setValueSchemaFile(valueSchemaFile);
        }
    }

    void addKafkaTopicToTopology(String pathname, List<String> domainNames, Set<TopologyFile> topologies, Collection<String> subjects, KafkaTopic kafkaTopic) {
        final Pattern pattern = Pattern.compile(ApplicationConfiguration.REGEX_FULL_TOPIC_NAME);
        final Matcher matcher = pattern.matcher(kafkaTopic.getName());
        if (matcher.matches() && matcher.groupCount() == 3 && domainNames.contains(matcher.group(1))) {
            final String domainName = matcher.group(1);
            final String topicName = matcher.group(3);
            final String keySchemaFile = schemaFileService.findSchema(subjects, domainName, kafkaTopic.getName(), "-key");
            final String valueSchemaFile = schemaFileService.findSchema(subjects, domainName, kafkaTopic.getName(), "-value");
            final TopologyFile topology = createTopologyIfNotExists(pathname, domainName, topologies);
            final Domain domain = createDomainIfNotExistsInTopology(matcher.group(1), topology);
            final Visibility visibility = createVisibilityIfNotExistsInDomain(matcher.group(2), domain);
            final Topic topic = topicService.createTopic(topicName, kafkaTopic);
            topic.setKeySchemaFile(keySchemaFile);
            topic.setValueSchemaFile(valueSchemaFile);
            visibility.getTopics().add(topic);
        }
    }

    TopologyFile createTopologyIfNotExists(final String pathname, final String domainName, final Set<TopologyFile> topologies) {
        TopologyFile topology = topologies.stream()
                .filter(t -> isTopologyFileMatch(domainName, t))
                .findFirst()
                .orElse(null);
        if (Objects.isNull(topology)) {
            topology = new TopologyFile();
            topology.setFile(new File(pathname, "topology-" + domainName + ".yaml"));
            topologies.add(topology);
        }

        return topology;
    }

    private boolean isTopologyFileMatch(String domainName, TopologyFile topology) {
        final File file = topology.getFile();
        if (Objects.isNull(file)) {
            return false;
        }

        return Objects.equals(file.getName(), "topology-" + domainName + ".yaml");
    }

    Domain createDomainIfNotExistsInTopology(final String domainName, final TopologyFile topology) {
        Domain domain = topology.getDomain();
        if (Objects.isNull(domain)) {
            domain = new Domain(domainName);
            topology.setDomain(domain);
        }

        return domain;
    }

    Visibility createVisibilityIfNotExistsInDomain(final String visibilityType, final Domain domain) {
        final Visibility.Type type = Visibility.Type.valueOf(visibilityType.toUpperCase());
        Visibility visibility = domain.getVisibilities().stream()
                .filter(v -> Objects.equals(type, v.getType()))
                .findFirst()
                .orElse(null);
        if (Objects.isNull(visibility)) {
            visibility = new Visibility();
            visibility.setType(type);
            domain.getVisibilities().add(visibility);
        }

        return visibility;
    }

    String getValueSchemaFilename(String domainName, Topic topic) {
        return ApplicationConfiguration.EVENTS_DIRECTORY
                + "/" + domainName
                + "/" + topic.getFullName()
                + ApplicationConfiguration.SCHEMA_FILE_VALUE_SUFFIX;
    }

    List<Triplet<String, String, Topic>> getDomainNameAndVisibilityFullNameAndTopics(Pair<String, Visibility> pair) {
        return pair.getValue1().getTopics().stream()
                .map(topic -> Triplet.with(pair.getValue0(), pair.getValue1().getFullName(), topic))
                .collect(Collectors.toUnmodifiableList());
    }

    List<Pair<String, Visibility>> getDomainNameAndVisibilities(Domain domain) {
        return domain.getVisibilities().stream()
                .map(visibility -> Pair.with(domain.getName(), visibility))
                .collect(Collectors.toUnmodifiableList());
    }

    List<Domain> listDomains(Set<TopologyFile> topologies) {
        return topologies.stream()
                .map(TopologyFile::getDomain)
                .collect(Collectors.toUnmodifiableList());
    }

    Set<TopologyFile> listTopologiesToBeRestored(String pathname, List<String> domainNames) throws ExecutionException, InterruptedException {
        final Set<TopologyFile> topologies = new HashSet<>();
        final List<KafkaTopic> kafkaTopics = topicService.listTopicsInCluster();
        final Collection<String> subjects = schemaFileService.listSubjects();
        if (Objects.isNull(kafkaTopics) || kafkaTopics.isEmpty() || Objects.isNull(subjects) || subjects.isEmpty()) {
            LOGGER.info("No topics to restore from cluster.");
            return Collections.emptySet();
        }
        kafkaTopics.forEach(kafkaTopic -> addKafkaTopicToTopology(pathname, domainNames, topologies, subjects, kafkaTopic));

        return Collections.unmodifiableSet(topologies);
    }

}
