package de.volkerfaas.kafka.topology.repositories;

import de.volkerfaas.kafka.topology.model.KafkaCluster;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.resource.ResourceType;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public interface KafkaClusterRepository {

    void createAccessControlLists(Collection<AclBinding> aclBindings) throws ExecutionException, InterruptedException;
    void createPartitions(Map<String, NewPartitions> newPartitions) throws ExecutionException, InterruptedException;
    void createTopics(Set<NewTopic> newTopics) throws ExecutionException, InterruptedException;
    void deleteAccessControlLists(Collection<AclBindingFilter> aclBindingFilters);
    KafkaCluster dumpCluster() throws ExecutionException, InterruptedException;
    Collection<AclBinding> listAccessControlLists(ResourceType resourceType, String name) throws ExecutionException, InterruptedException;
    void updateConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs) throws ExecutionException, InterruptedException;

}
