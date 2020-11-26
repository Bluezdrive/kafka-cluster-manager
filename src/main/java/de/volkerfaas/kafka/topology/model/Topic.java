package de.volkerfaas.kafka.topology.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.volkerfaas.kafka.topology.validation.HasValidPartitionIncrementation;
import de.volkerfaas.kafka.topology.validation.ValidSchemaContent;
import de.volkerfaas.kafka.topology.validation.ValidTopicConfig;

import javax.validation.Valid;
import javax.validation.constraints.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static de.volkerfaas.kafka.topology.ApplicationConfiguration.REGEX_TOPIC_NAME;

@HasValidPartitionIncrementation
public class Topic implements ConsumerAccessControl {

    private String prefix;
    private String name;
    private String description;
    private final List<AccessControl> consumers;
    private int numPartitions = 6;
    private short replicationFactor = 3;
    private Schema keySchema;
    private Schema valueSchema;
    private final Map<String, String> config;

    public Topic() {
        this.consumers = new ArrayList<>();
        this.config = new HashMap<>();
    }

    public Topic(String name, int numPartitions, short replicationFactor, Map<String, String> config) {
        this();
        this.name = name;
        this.numPartitions = numPartitions;
        this.replicationFactor = replicationFactor;
        this.config.putAll(config);
    }

    @JsonIgnore
    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    @NotNull
    @Pattern(
            message = "must be either something like 'user_created', 'create_user' or 'users'",
            regexp = "^" + REGEX_TOPIC_NAME + "$"
    )
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @NotBlank
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @JsonIgnore
    @Override
    public String getFullName() {
        return this.prefix + this.name;
    }

    @Valid
    @Override
    public List<AccessControl> getConsumers() {
        return consumers;
    }

    @ValidTopicConfig
    public Map<String, String> getConfig() {
        return config;
    }

    @Min(1)
    @Max(20)
    public int getNumPartitions() {
        return numPartitions;
    }

    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public short getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(short replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    @Valid
    @ValidSchemaContent
    public Schema getKeySchema() {
        return keySchema;
    }

    public void setKeySchema(Schema keySchema) {
        this.keySchema = keySchema;
    }

    @NotNull
    @Valid
    @ValidSchemaContent
    public Schema getValueSchema() {
        return valueSchema;
    }

    public void setValueSchema(Schema valueSchema) {
        this.valueSchema = valueSchema;
    }

    @JsonIgnore
    public Set<Schema> getSchemas() {
        return Stream.of(keySchema, valueSchema).filter(Objects::nonNull).collect(Collectors.toSet());
    }

    @Override
    public String toString() {
        return "Topic{" +
                "prefix='" + prefix + "'," +
                "name='" + name + "'," +
                "fullName='" + getFullName() + "'," +
                "consumers=[" + this.consumers.stream().map(AccessControl::toString).collect(Collectors.joining(",")) + "]," +
                "numPartitions=" + numPartitions + "," +
                "replicationFactor=" + replicationFactor + "," +
                "keySchemaFile=" + keySchema + "," +
                "valueSchemaFile=" + valueSchema + "," +
                "config=" + config +
                '}';
    }
}
