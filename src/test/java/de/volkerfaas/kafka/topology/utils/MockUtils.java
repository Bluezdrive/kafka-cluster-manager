package de.volkerfaas.kafka.topology.utils;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public final class MockUtils {

    private MockUtils() {
        throw new AssertionError("No de.volkerfaas.kafka.topology.utils.MockUtils instances for you!");
    }

    public static void mockListOffsets(AdminClient adminClient, Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> topicPartitionListOffsetsResultInfos) throws ExecutionException, InterruptedException {
        final KafkaFuture<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> kafkaFuture = mock(KafkaFuture.class);
        doReturn(topicPartitionListOffsetsResultInfos).when(kafkaFuture).get();
        final ListOffsetsResult listOffsetsResult = mock(ListOffsetsResult.class);
        doReturn(kafkaFuture).when(listOffsetsResult).all();
        doReturn(listOffsetsResult).when(adminClient).listOffsets(anyMap());
    }

    public static void mockListConsumerGroups(AdminClient adminClient, Collection<ConsumerGroupListing> consumerGroupListings) throws ExecutionException, InterruptedException {
        final KafkaFuture<Collection<ConsumerGroupListing>> kafkaFuture = mock(KafkaFuture.class);
        doReturn(consumerGroupListings).when(kafkaFuture).get();
        final ListConsumerGroupsResult listConsumerGroupsResult = mock(ListConsumerGroupsResult.class);
        doReturn(kafkaFuture).when(listConsumerGroupsResult).all();
        doReturn(listConsumerGroupsResult).when(adminClient).listConsumerGroups();
    }

    public static void mockDescribeConsumerGroups(AdminClient adminClient, Map<String, ConsumerGroupDescription> consumerGroupDescriptions) throws ExecutionException, InterruptedException {
        final KafkaFuture<Map<String, ConsumerGroupDescription>> kafkaFuture = mock(KafkaFuture.class);
        doReturn(consumerGroupDescriptions).when(kafkaFuture).get();
        final DescribeConsumerGroupsResult describeConsumerGroupsResult = mock(DescribeConsumerGroupsResult.class);
        doReturn(kafkaFuture).when(describeConsumerGroupsResult).all();
        doReturn(describeConsumerGroupsResult).when(adminClient).describeConsumerGroups(anyCollection());
    }

    public static void mockDeleteAcls(AdminClient adminClient, Set<AclBindingFilter> expectedAclBindingFilters) throws InterruptedException, ExecutionException {
        final KafkaFuture<Void> kafkaFuture = mock(KafkaFuture.class);
        doReturn(null).when(kafkaFuture).get();
        final DeleteAclsResult deleteAclsResult = mock(DeleteAclsResult.class);
        doReturn(kafkaFuture).when(deleteAclsResult).all();
        doAnswer(invocation -> {
            final Collection<AclBindingFilter> filters = invocation.getArgument(0);
            assertNotNull(filters);
            assertEquals(expectedAclBindingFilters.size(), filters.size());
            assertThat(filters, containsInAnyOrder(expectedAclBindingFilters.toArray()));
            return deleteAclsResult;
        }).when(adminClient).deleteAcls(anyCollection());
    }

    public static void mockDeleteTopics(AdminClient adminClient, Collection<String> expectedTopics) throws ExecutionException, InterruptedException {
        final KafkaFuture<Void> kafkaFuture = mock(KafkaFuture.class);
        doReturn(null).when(kafkaFuture).get();
        final DeleteTopicsResult deleteTopicsResult = mock(DeleteTopicsResult.class);
        doReturn(kafkaFuture).when(deleteTopicsResult).all();
        doAnswer(invocation -> {
            final Collection<String> topics = invocation.getArgument(0);
            assertNotNull(topics);
            assertEquals(expectedTopics.size(), topics.size());
            assertThat(topics, containsInAnyOrder(expectedTopics.toArray()));

            return deleteTopicsResult;
        }).when(adminClient).deleteTopics(anyCollection());
    }

    public static void mockParseSchema(SchemaRegistryClient schemaRegistryClient, String schemaString) {
        doReturn(Optional.of(new AvroSchema(schemaString))).when(schemaRegistryClient).parseSchema(eq(AvroSchema.TYPE), anyString(), eq(Collections.emptyList()));
    }

    public static void mockListTopics(AdminClient adminClient, Set<String> topicNames) throws InterruptedException, ExecutionException {
        final KafkaFuture<Set<String>> kafkaFutureNames = mock(KafkaFuture.class);
        final ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
        doReturn(topicNames).when(kafkaFutureNames).get();
        doReturn(kafkaFutureNames).when(listTopicsResult).names();
        doReturn(listTopicsResult).when(adminClient).listTopics();
    }

    public static void mockDescribeTopics(AdminClient adminClient, Map<String, TopicDescription> topicDescriptions) throws InterruptedException, ExecutionException {
        final KafkaFuture<Map<String, TopicDescription>> kafkaFutureTopicDescription = mock(KafkaFuture.class);
        final DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
        doReturn(topicDescriptions).when(kafkaFutureTopicDescription).get();
        doReturn(kafkaFutureTopicDescription).when(describeTopicsResult).all();
        doReturn(describeTopicsResult).when(adminClient).describeTopics(anyCollection());
    }

    public static void mockDescribeConfigs(AdminClient adminClient, Map<ConfigResource, Config> configMap) throws InterruptedException, ExecutionException {
        final KafkaFuture<Map<ConfigResource, Config>> kafkaFutureConfig = mock(KafkaFuture.class);
        final DescribeConfigsResult describeConfigsResult = mock(DescribeConfigsResult.class);
        doReturn(configMap).when(kafkaFutureConfig).get();
        doReturn(kafkaFutureConfig).when(describeConfigsResult).all();
        doReturn(describeConfigsResult).when(adminClient).describeConfigs(anySet());
    }

    public static void mockDescribeAcls(AdminClient adminClient, Set<AclBinding> aclBindings) throws InterruptedException, ExecutionException {
        final KafkaFuture<Set<AclBinding>> kafkaFutureAclBindings = mock(KafkaFuture.class);
        final DescribeAclsResult describeAclsResult = mock(DescribeAclsResult.class);
        doReturn(aclBindings).when(kafkaFutureAclBindings).get();
        doReturn(kafkaFutureAclBindings).when(describeAclsResult).values();
        doReturn(describeAclsResult).when(adminClient).describeAcls(any());
    }

    public static void mockDescribeCluster(AdminClient adminClient, String clusterId) throws InterruptedException, ExecutionException {
        final KafkaFuture<String> kafkaFutureClusterId = mock(KafkaFuture.class);
        doReturn(clusterId).when(kafkaFutureClusterId).get();
        final DescribeClusterResult describeClusterResult = mock(DescribeClusterResult.class);
        doReturn(kafkaFutureClusterId).when(describeClusterResult).clusterId();
        doReturn(describeClusterResult).when(adminClient).describeCluster();
    }

    public static void mockListConsumerGroupOffsets(AdminClient adminClient, Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadata) throws ExecutionException, InterruptedException {
        final KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> kafkaFuture = mock(KafkaFuture.class);
        doReturn(topicPartitionOffsetAndMetadata).when(kafkaFuture).get();
        final ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult = mock(ListConsumerGroupOffsetsResult.class);
        doReturn(kafkaFuture).when(listConsumerGroupOffsetsResult).partitionsToOffsetAndMetadata();
        doReturn(listConsumerGroupOffsetsResult).when(adminClient).listConsumerGroupOffsets(anyString());
    }

}
