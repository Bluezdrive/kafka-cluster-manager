package de.volkerfaas.kafka.topology.services.impl;

import de.volkerfaas.kafka.cluster.model.TopicConfiguration;
import de.volkerfaas.kafka.topology.model.Domain;
import de.volkerfaas.kafka.topology.model.Schema;
import de.volkerfaas.kafka.topology.model.TopologyFile;
import de.volkerfaas.kafka.topology.model.Visibility;
import de.volkerfaas.kafka.topology.repositories.TopologyFileRepository;
import de.volkerfaas.kafka.topology.services.*;
import de.volkerfaas.kafka.topology.validation.impl.ValidatorPayload;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.hibernate.validator.HibernateValidatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class TopologyDeployServiceImpl implements TopologyDeployService {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopologyDeployServiceImpl.class);

    private final AccessControlService accessControlService;
    private final SchemaFileService schemaFileService;
    private final TopicService topicService;
    private final TopologyValuesService topologyValuesService;
    private final TopologyFileRepository topologyFileRepository;
    private final Validator validator;

    @Autowired
    public TopologyDeployServiceImpl(final AccessControlService accessControlService, final SchemaFileService schemaFileService, final TopicService topicService, final TopologyValuesService topologyValuesService, final TopologyFileRepository topologyFileRepository, final Validator validator) {
        this.accessControlService = accessControlService;
        this.schemaFileService = schemaFileService;
        this.topicService = topicService;
        this.topologyValuesService = topologyValuesService;
        this.topologyFileRepository = topologyFileRepository;
        this.validator = validator;
    }

    @Override
    public void deleteOrphanedAclBindings(final Collection<Domain> domains) throws ExecutionException, InterruptedException {
        final Collection<AclBindingFilter> orphanedAclBindings = accessControlService.listOrphanedAclBindings(domains);
        accessControlService.deleteAccessControlLists(orphanedAclBindings);
    }

    @Override
    public void deleteOrphanedSubjects(final Collection<Domain> domains) throws IOException, RestClientException {
        final Set<String> topicNames = topicService.listTopicNames(domains);
        final Collection<String> orphanedSubjects = schemaFileService.listOrphanedSubjects(topicNames);
        schemaFileService.deleteSubjects(orphanedSubjects);
    }

    @Override
    public void deleteOrphanedTopics(final Collection<Domain> domains) throws ExecutionException, InterruptedException {
        final Collection<String> orphanedTopics = topicService.listOrphanedTopics(domains);
        topicService.deleteTopics(orphanedTopics);
    }

    @Override
    public boolean isTopologyValid(final Collection<TopologyFile> topologies, final String directory) throws ExecutionException, InterruptedException {
        final Collection<TopicConfiguration> topicConfigurations = topicService.listTopicsInCluster();
        final ValidatorPayload validatorPayload = new ValidatorPayload(directory, topologies, topicConfigurations);
        final HibernateValidatorFactory validatorFactory = this.validator.unwrap(HibernateValidatorFactory.class);
        final Validator hibernateValidator = validatorFactory.usingContext().constraintValidatorPayload(validatorPayload).getValidator();
        final Set<ConstraintViolation<TopologyFile>> violations = topologies.stream()
                .peek(topology -> LOGGER.info("Validating topology '{}'", topology.getFile()))
                .map((Function<TopologyFile, Set<ConstraintViolation<TopologyFile>>>) hibernateValidator::validate)
                .flatMap(Set::stream)
                .peek(violation -> LOGGER.error("{} {}", violation.getPropertyPath(), violation.getMessage()))
                .collect(Collectors.toUnmodifiableSet());
        if (violations.isEmpty()) {
            LOGGER.info("Topologies have been validated without constraints");
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Set<TopologyFile> listTopologies(final String directory) {
        return topologyFileRepository.listTopologyFiles(directory).stream()
                .map(topologyFileRepository::readTopology)
                .filter(Objects::nonNull)
                .map(topologyValuesService::addAdditionalValues)
                .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public Collection<Domain> filterDomainsForUpdate(final Collection<TopologyFile> topologies, final Collection<String> domainNames) {
        return topologies.stream()
                .map(TopologyFile::getDomain)
                .filter(Objects::nonNull)
                .filter(domain -> domainNames.isEmpty() || domainNames.contains(domain.getName()))
                .peek(domain -> LOGGER.info("Domain '{}' is to be updated", domain.getName()))
                .collect(Collectors.toUnmodifiableList());
    }

    public void removeTopicsNotInCluster(final Collection<TopologyFile> topologies, String cluster) {
        topologies.stream()
                .map(TopologyFile::getDomain)
                .map(Domain::getVisibilities)
                .flatMap(List::stream)
                .map(Visibility::getTopics)
                .forEach(topics -> topics.removeIf(topic -> !topic.getClusters().contains(cluster)));
    }

    @Override
    public void updateTopology(final Collection<Domain> domains, final String directory) throws ExecutionException, InterruptedException {
        if (domains.isEmpty()) {
            return;
        }
        final Collection<NewTopic> newTopics = topicService.createNewTopics(domains);
        final Collection<AclBinding> newAclBindings = accessControlService.listNewAclBindings(domains);
        final Map<String, NewPartitions> newPartitions = topicService.createNewPartitions(domains);
        final Map<ConfigResource, Collection<AlterConfigOp>> alterConfigOperations = topicService.createAlterConfigOperations(domains);
        final Collection<Schema> schemas = schemaFileService.listSchemasByDomains(domains);
        schemaFileService.registerSchemas(schemas, directory);
        topicService.createTopics(newTopics);
        accessControlService.createAccessControlLists(newAclBindings);
        topicService.createPartitions(newPartitions);
        topicService.updateConfigs(alterConfigOperations);
    }

}
