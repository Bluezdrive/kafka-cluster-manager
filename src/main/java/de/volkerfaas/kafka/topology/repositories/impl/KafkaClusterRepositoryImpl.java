package de.volkerfaas.kafka.topology.repositories.impl;

import de.volkerfaas.kafka.topology.ApplicationConfiguration;
import de.volkerfaas.kafka.topology.model.KafkaCluster;
import de.volkerfaas.kafka.topology.model.KafkaTopic;
import de.volkerfaas.kafka.topology.repositories.KafkaClusterRepository;
import de.volkerfaas.kafka.topology.utils.ConfigEntries;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Repository
public class KafkaClusterRepositoryImpl implements KafkaClusterRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClusterRepositoryImpl.class);

    private final Environment environment;
    private final AdminClient adminClient;

    @Autowired
    public KafkaClusterRepositoryImpl(Environment environment, @Lazy AdminClient adminClient) {
        this.environment = environment;
        this.adminClient = adminClient;
    }

    @Override
    public void createAccessControlLists(Collection<AclBinding> aclBindings) throws ExecutionException, InterruptedException {
        if (Objects.isNull(aclBindings) || aclBindings.isEmpty()) {
            LOGGER.info("No new ACLs to be created in cluster");
            return;
        }
        if (isNotDryRun()) {
            adminClient.createAcls(aclBindings).all().get();
            LOGGER.info("New ACLs created in cluster: {}", aclBindings);
        } else {
            LOGGER.info("New ACLs to be created in cluster: {}", aclBindings);
        }
    }



    @Override
    public void createPartitions(Map<String, NewPartitions> newPartitions) throws ExecutionException, InterruptedException {
        if (Objects.isNull(newPartitions) || newPartitions.isEmpty()) {
            LOGGER.info("No new partitions to be created in cluster");
            return;
        }
        if (isNotDryRun()) {
            adminClient.createPartitions(newPartitions).all().get();
            LOGGER.info("New partitions created in cluster: {}", newPartitions);
        } else {
            LOGGER.info("New topics to be created in cluster: {}", newPartitions);
        }
    }

    @Override
    public void createTopics(Set<NewTopic> newTopics) throws ExecutionException, InterruptedException {
        if (Objects.isNull(newTopics) || newTopics.isEmpty()) {
            LOGGER.info("No new topics to be created in cluster");
            return;
        }
        if (isNotDryRun()) {
            adminClient.createTopics(newTopics).all().get();
            LOGGER.info("New topics created in cluster: {}", newTopics);
        } else {
            LOGGER.info("New topics to be created in cluster: {}", newTopics);
        }
    }

    @Override
    public void deleteAccessControlLists(Collection<AclBindingFilter> aclBindingFilters) {
        if (Objects.isNull(aclBindingFilters) || aclBindingFilters.isEmpty()) {
            LOGGER.info("No orphaned ACLs to be removed from cluster");
            return;
        }
        if (isNotDryRun()) {
            adminClient.deleteAcls(aclBindingFilters);
            LOGGER.info("Orphaned ACLs removed from cluster: {}", aclBindingFilters);
        } else {
            LOGGER.info("Orphaned ACLs to be removed from cluster: {}", aclBindingFilters);
        }
    }

    @Override
    @Cacheable("cluster")
    public KafkaCluster dumpCluster() throws ExecutionException, InterruptedException {
        final String clusterId = adminClient.describeCluster().clusterId().get();
        LOGGER.info("Connected to Apache Kafka® cluster '{}'", clusterId);

        final KafkaCluster cluster = new KafkaCluster(clusterId);
        final Set<String> topicNames = listTopicNames();
        cluster.getTopics().addAll(listTopicsByNames(topicNames));
        cluster.getAclBindings().addAll(listAccessControlLists(ResourceType.ANY, null));

        return cluster;
    }

    @Override
    public Set<AclBinding> listAccessControlLists(ResourceType resourceType, String name) throws ExecutionException, InterruptedException {
        ResourcePatternFilter resourcePatternFilter = new ResourcePatternFilter(resourceType, name, PatternType.ANY);
        AclBindingFilter aclBindingFilter = new AclBindingFilter(resourcePatternFilter, AccessControlEntryFilter.ANY);
        final Collection<AclBinding> aclBindings = adminClient
                .describeAcls(aclBindingFilter)
                .values()
                .get();

        LOGGER.debug("Received access control lists from Apache Kafka® cluster for resource '{}': {}", name != null ? name : "any", aclBindings);

        return aclBindings.stream()
                .filter(aclBinding -> name == null || Objects.equals(name, aclBinding.pattern().name()))
                .collect(Collectors.toSet());
    }

    @Override
    public void updateConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs) throws ExecutionException, InterruptedException {
        if (Objects.isNull(configs) || configs.isEmpty()) {
            LOGGER.info("No config items to be updated in cluster");
            return;
        }
        if (isNotDryRun()) {
            adminClient.incrementalAlterConfigs(configs).all().get();
            LOGGER.info("Config items updated in cluster: {}", configs);
        } else {
            LOGGER.info("Config items to be updated in cluster: {}", configs);
        }
    }

    short getReplicationFactor(TopicDescription topicDescription) {
        return (short) topicDescription
                .partitions()
                .get(0)
                .replicas()
                .size();
    }


    boolean isDynamicTopicConfig(ConfigEntry configEntry) {
        return configEntry.source().equals(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG);
    }

    boolean isNotDryRun() {
        return !environment.getRequiredProperty(ApplicationConfiguration.PROPERTY_KEY_TOPOLOGY_DRY_RUN, boolean.class);
    }

    Set<String> listTopicNames() throws ExecutionException, InterruptedException {
        Set<String> topicNames = adminClient
                .listTopics()
                .names()
                .get();
        LOGGER.debug("Received topic names from Apache Kafka® cluster: {}", topicNames);

        return topicNames;
    }

    List<KafkaTopic> listTopicsByNames(Set<String> names) throws ExecutionException, InterruptedException {
        final Map<String, TopicDescription> topicDescriptions = adminClient.describeTopics(names).all().get();
        LOGGER.debug("Received topic descriptions from Apache Kafka® cluster: {}", topicDescriptions);
        final Map<String, Config> configs = listConfigsByNames(names);
        LOGGER.debug("Received topic configurations from Apache Kafka® cluster: {}", configs);

        return topicDescriptions.values().stream()
                .map(topicDescription -> {
                    final String name = topicDescription.name();
                    final Config config = configs.get(name);
                    Map<String, String> topicConfig = config.entries().stream()
                            .filter(this::isDynamicTopicConfig)
                            .collect(Collectors.toMap(ConfigEntries::getCamelCase, ConfigEntry::value));

                    return new KafkaTopic(name, topicDescription.partitions().size(), getReplicationFactor(topicDescription), topicConfig);
                })
                .collect(Collectors.toList());
    }

    Map<String, Config> listConfigsByNames(Set<String> names) throws ExecutionException, InterruptedException {
        Set<ConfigResource> resources = names.stream()
                .map(name -> new ConfigResource(ConfigResource.Type.TOPIC, name))
                .collect(Collectors.toSet());
        Map<ConfigResource, Config> configs = adminClient
                .describeConfigs(resources)
                .all()
                .get();
        LOGGER.debug("Received topic configs from Apache Kafka® cluster: {}", configs);

        return configs.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> e.getKey().name(),
                        Map.Entry::getValue
                ));
    }

}
