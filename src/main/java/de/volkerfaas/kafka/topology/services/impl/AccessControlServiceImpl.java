package de.volkerfaas.kafka.topology.services.impl;

import de.volkerfaas.kafka.cluster.model.ClusterConfiguration;
import de.volkerfaas.kafka.cluster.repositories.KafkaClusterRepository;
import de.volkerfaas.kafka.topology.ApplicationConfiguration;
import de.volkerfaas.kafka.topology.model.AccessControl;
import de.volkerfaas.kafka.topology.model.ConsumerAccessControl;
import de.volkerfaas.kafka.topology.model.Domain;
import de.volkerfaas.kafka.topology.model.Visibility;
import de.volkerfaas.kafka.topology.services.AccessControlService;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class AccessControlServiceImpl implements AccessControlService {

    private final KafkaClusterRepository kafkaClusterRepository;

    @Autowired
    public AccessControlServiceImpl(KafkaClusterRepository kafkaClusterRepository) {
        this.kafkaClusterRepository = kafkaClusterRepository;
    }

    @Override
    public void createAccessControlLists(Collection<AclBinding> aclBindings) throws ExecutionException, InterruptedException {
        kafkaClusterRepository.createAccessControlLists(aclBindings);
    }

    @Override
    public String findPrincipalByResourceName(Collection<AclBinding> aclBindings, String resourceName) {
        return aclBindings.stream()
                .filter(aclBinding -> Objects.equals(aclBinding.pattern().name(), resourceName))
                .map(aclBinding -> aclBinding.entry().principal())
                .findFirst()
                .orElse(null);
    }

    @Override
    public Collection<String> findPrincipalsByResourceName(Collection<AclBinding> aclBindings, String resourceName) {
        return aclBindings.stream()
                .filter(aclBinding -> Objects.equals(aclBinding.pattern().name(), resourceName))
                .map(aclBinding -> aclBinding.entry().principal())
                .distinct()
                .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public Collection<AclBinding> deleteAccessControlLists(Collection<AclBindingFilter> aclBindingFilters) throws ExecutionException, InterruptedException {
        return kafkaClusterRepository.deleteAccessControlLists(aclBindingFilters);
    }

    @Override
    public Collection<AclBinding> listAclBindingsInCluster() throws ExecutionException, InterruptedException {
        final ClusterConfiguration clusterConfiguration = kafkaClusterRepository.getClusterConfiguration();
        if (Objects.isNull(clusterConfiguration)) {
            return Collections.emptyList();
        }

        return Collections.unmodifiableCollection(clusterConfiguration.getAclBindings());
    }

    @Override
    public Collection<AclBinding> listNewAclBindings(Collection<Domain> domains) {
        final Set<AclBinding> newAclBindings = new HashSet<>();
        domains.stream()
                .peek(domain -> newAclBindings.addAll(listAclBindingsForDomain(domain)))
                .map(Domain::getVisibilities)
                .flatMap(List::stream)
                .peek(visibility -> newAclBindings.addAll(listAclBindingsForVisibilityOrTopic(visibility, true)))
                .map(Visibility::getTopics)
                .flatMap(List::stream)
                .forEach(topic -> newAclBindings.addAll(listAclBindingsForVisibilityOrTopic(topic, false)));
        return newAclBindings;
    }

    @Override
    public Collection<AclBindingFilter> listOrphanedAclBindings(Collection<Domain> domains) throws ExecutionException, InterruptedException {
        final Set<AclBindingFilter> orphanedAclBindings = new HashSet<>();
        domains.stream()
                .peek(domain -> orphanedAclBindings.addAll(listOrphanedAclBindingFilters(domain)))
                .map(Domain::getVisibilities)
                .flatMap(List::stream)
                .peek(visibility -> orphanedAclBindings.addAll(listOrphanedAclBindingFilters(visibility, true)))
                .map(Visibility::getTopics)
                .flatMap(List::stream)
                .forEach(topic -> orphanedAclBindings.addAll(listOrphanedAclBindingFilters(topic, false)));
        orphanedAclBindings.addAll(listAclBindingFiltersNotInDomains(domains));

        return orphanedAclBindings;
    }

    public AclBinding getAclBinding(ResourceType type, String name, String principal, AclOperation operation, boolean prefix) {
        final ResourcePattern resourcePattern = new ResourcePattern(type, name, prefix ? PatternType.PREFIXED : PatternType.LITERAL);
        final AccessControlEntry accessControlEntry = new AccessControlEntry(principal, "*", operation, AclPermissionType.ALLOW);

        return new AclBinding(resourcePattern, accessControlEntry);
    }

    public String getAclBindingFilterPrincipal(Set<AclBindingFilter> aclBindingFilters) {
        return aclBindingFilters.stream()
                .map(aclBindingFilter -> aclBindingFilter.entryFilter().principal())
                .findFirst()
                .orElse(null);
    }

    public String getResourceName(ConsumerAccessControl consumerAccessControl, boolean prefix) {
        return consumerAccessControl.getFullName() + (prefix ? "." : "");
    }

    public boolean isAclBindingNotInDomains(AclBinding aclBinding, Set<String> domainNames) {
        final String principal = aclBinding.pattern().name();
        final Pattern pattern = Pattern.compile(ApplicationConfiguration.REGEX_DOMAIN + ".*");
        final Matcher matcher = pattern.matcher(principal);
        if (matcher.matches()) {
            final String group = matcher.group(1);

            return !domainNames.contains(group);
        } else {
            return false;
        }
    }

    public boolean isAclNotAvailable(Collection<AclBinding> aclBindings, AclBinding aclBinding) {
        return aclBindings.stream().noneMatch(aclBinding::equals);
    }

    public Set<AclBindingFilter> listAclBindingFilters(String principal, ResourceType resourceType, AclOperation aclOperation) throws ExecutionException, InterruptedException {
        if (Objects.isNull(principal)) {
            return Collections.emptySet();
        }
        final ClusterConfiguration clusterConfiguration = kafkaClusterRepository.getClusterConfiguration();
        final Collection<AclBinding> aclBindings = clusterConfiguration.getAclBindings();

        return aclBindings.stream()
                .filter(aclBinding -> aclBinding.entry().principal().equals(principal)
                        && aclBinding.pattern().resourceType().equals(resourceType)
                        && aclBinding.entry().permissionType().equals(AclPermissionType.ALLOW)
                        && aclBinding.entry().operation().equals(aclOperation))
                .map(AclBinding::toFilter)
                .collect(Collectors.toSet());
    }

    public Set<AclBindingFilter> listAclBindingFiltersNotInDomains(Collection<Domain> domains) throws ExecutionException, InterruptedException {
        final ClusterConfiguration clusterConfiguration = kafkaClusterRepository.getClusterConfiguration();
        final Collection<AclBinding> aclBindings = clusterConfiguration.getAclBindings();
        final Set<String> domainNames = domains.stream().map(Domain::getName).collect(Collectors.toSet());

        final Set<AclBindingFilter> aclBindingFilters = aclBindings.stream().filter(aclBinding -> isAclBindingNotInDomains(aclBinding, domainNames))
                .map(AclBinding::toFilter)
                .collect(Collectors.toSet());

        final String aclBindingFilterPrincipal = getAclBindingFilterPrincipal(aclBindingFilters);
        aclBindingFilters.addAll(listAclBindingFilters(aclBindingFilterPrincipal, ResourceType.CLUSTER, AclOperation.IDEMPOTENT_WRITE));

        return aclBindingFilters;
    }

    public Set<AclBinding> listAclBindingsByName(String name) throws ExecutionException, InterruptedException {
        if (Objects.isNull(name)) {
            return Collections.emptySet();
        }
        final ClusterConfiguration clusterConfiguration = kafkaClusterRepository.getClusterConfiguration();
        final Collection<AclBinding> aclBindings = clusterConfiguration.getAclBindings();

        return aclBindings.stream()
                .filter(aclBinding -> Objects.equals(aclBinding.pattern().name(), name))
                .collect(Collectors.toSet());
    }

    public Set<AclBinding> listAclBindingsForConsumer(String resourceName, String principal, boolean prefix) {
        final Set<AclBinding> consumerAclBindings = new HashSet<>();
        if (Strings.isNotBlank(principal)) {
            try {
                final ClusterConfiguration clusterConfiguration = kafkaClusterRepository.getClusterConfiguration();
                final Collection<AclBinding> aclBindings = clusterConfiguration.getAclBindings();
                final AclBinding aclBindingTopicDescribe = getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.DESCRIBE, prefix);
                if (isAclNotAvailable(aclBindings, aclBindingTopicDescribe)) {
                    consumerAclBindings.add(aclBindingTopicDescribe);
                }
                final AclBinding aclBindingTopicRead = getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.READ, prefix);
                if (isAclNotAvailable(aclBindings, aclBindingTopicRead)) {
                    consumerAclBindings.add(aclBindingTopicRead);
                }
                final AclBinding aclBindingGroupRead = getAclBinding(ResourceType.GROUP, resourceName, principal, AclOperation.READ, prefix);
                if (isAclNotAvailable(aclBindings, aclBindingGroupRead)) {
                    consumerAclBindings.add(aclBindingGroupRead);
                }
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
        return consumerAclBindings;
    }

    public Set<AclBinding> listAclBindingsForDomain(Domain domain) {
        final String resourceName = domain.getName() + ".";
        final String principal = domain.getPrincipal();
        final Set<AclBinding> domainAclBindings = new HashSet<>();
        try {
            domainAclBindings.addAll(listAclBindingsForConsumer(resourceName, principal, true));
            domainAclBindings.addAll(listAclBindingsForProducer(resourceName, principal));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

        return domainAclBindings;
    }

    public Set<AclBinding> listAclBindingsForProducer(String resourceName, String principal) {
        final Set<AclBinding> producerAclBindings = new HashSet<>();
        if (Strings.isNotBlank(principal)) {
            try {
                final ClusterConfiguration clusterConfiguration = kafkaClusterRepository.getClusterConfiguration();
                final Collection<AclBinding> aclBindings = clusterConfiguration.getAclBindings();
                final AclBinding aclBindingTopicWrite = getAclBinding(ResourceType.TOPIC, resourceName, principal, AclOperation.WRITE, true);
                if (isAclNotAvailable(aclBindings, aclBindingTopicWrite)) {
                    producerAclBindings.add(aclBindingTopicWrite);
                }

                final AclBinding aclBindingTransactionalIdWrite = getAclBinding(ResourceType.TRANSACTIONAL_ID, resourceName, principal, AclOperation.WRITE, true);
                if (isAclNotAvailable(aclBindings, aclBindingTransactionalIdWrite)) {
                    producerAclBindings.add(aclBindingTransactionalIdWrite);
                }

                final AclBinding aclBindingClusterIdempotentWrite = getAclBinding(ResourceType.CLUSTER, "kafka-cluster", principal, AclOperation.IDEMPOTENT_WRITE, false);
                if (isAclNotAvailable(aclBindings, aclBindingClusterIdempotentWrite)) {
                    producerAclBindings.add(aclBindingClusterIdempotentWrite);
                }
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
        return producerAclBindings;
    }

    public Set<AclBinding> listAclBindingsForVisibilityOrTopic(ConsumerAccessControl consumerAccessControl, boolean prefix) {
        final String resourceName = getResourceName(consumerAccessControl, prefix);
        return consumerAccessControl.getConsumers().stream()
                .map(consumer -> listAclBindingsForConsumer(resourceName, consumer.getPrincipal(), prefix))
                .flatMap(Set::stream)
                .collect(Collectors.toUnmodifiableSet());
    }

    public Set<String> listPrincipals(ConsumerAccessControl consumerAccessControl) {
        return consumerAccessControl.getConsumers().stream()
                .map(AccessControl::getPrincipal)
                .collect(Collectors.toSet());
    }

    public Set<AclBindingFilter> listOrphanedAclBindingFilters(Domain domain) {
        final String name = domain.getName() + ".";
        final String principal = domain.getPrincipal();
        try {
            final ClusterConfiguration clusterConfiguration = kafkaClusterRepository.getClusterConfiguration();
            final Collection<AclBinding> aclBindings = clusterConfiguration.getAclBindings();
            final Set<AclBindingFilter> aclBindingFilters = aclBindings.stream()
                    .filter(aclBinding -> {
                        final String aclBindingPrincipal = aclBinding.entry().principal();
                        final String aclBindingName = aclBinding.pattern().name();
                        return !Objects.equals(aclBindingPrincipal, principal)
                                && Objects.equals(aclBindingName, name);
                    })
                    .map(AclBinding::toFilter)
                    .collect(Collectors.toSet());
            final String aclBindingFilterPrincipal = getAclBindingFilterPrincipal(aclBindingFilters);
            aclBindingFilters.addAll(listAclBindingFilters(aclBindingFilterPrincipal, ResourceType.CLUSTER, AclOperation.IDEMPOTENT_WRITE));

            return aclBindingFilters;
        } catch (ExecutionException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    public Set<AclBindingFilter> listOrphanedAclBindingFilters(ConsumerAccessControl consumerAccessControl, boolean prefix) {
        final String name = consumerAccessControl.getFullName() + (prefix ? "." : "");
        final Set<String> principals = listPrincipals(consumerAccessControl);
        try {
            final Collection<AclBinding> aclBindings = listAclBindingsByName(name);
            return aclBindings.stream()
                    .filter(aclBinding -> !principals.contains(aclBinding.entry().principal()))
                    .map(AclBinding::toFilter)
                    .collect(Collectors.toUnmodifiableSet());
        } catch (ExecutionException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

}
