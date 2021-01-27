package de.volkerfaas.kafka.topology.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.volkerfaas.kafka.topology.validation.*;

import javax.validation.Valid;
import javax.validation.constraints.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static de.volkerfaas.kafka.topology.ApplicationConfiguration.REGEX_TOPIC_NAME;

@HasValidPartitionIncrementation
public class Topic implements ItemWithConsumers {

    private final List<String> clusters;
    private final Map<String, String> config;
    private final List<AccessControl> consumers;
    private String description;
    private Schema keySchema;
    private String name;
    private int numPartitions = 6;
    private short replicationFactor = 3;
    private Schema valueSchema;
    private int version;

    private String prefix;

    public Topic() {
        this.consumers = new ArrayList<>();
        this.config = new HashMap<>();
        this.clusters = new ArrayList<>();
    }

    public Topic(String name, int numPartitions, short replicationFactor, Map<String, String> config) {
        this();
        this.name = name;
        this.numPartitions = numPartitions;
        this.replicationFactor = replicationFactor;
        this.config.putAll(config);
    }

    public List<String> getClusters() {
        return clusters;
    }

    @ValidTopicConfig
    public Map<String, String> getConfig() {
        return config;
    }

    @Valid
    @Override
    public List<AccessControl> getConsumers() {
        return consumers;
    }

    @NotBlank
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Valid
    @ValidSchemaSubject(type = "key")
    @ValidSchemaContent
    @SchemaFileExists
    public Schema getKeySchema() {
        return keySchema;
    }

    public void setKeySchema(Schema keySchema) {
        this.keySchema = keySchema;
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

    @NotNull
    @Valid
    @ValidSchemaSubject
    @ValidSchemaContent
    @SchemaFileExists
    public Schema getValueSchema() {
        return valueSchema;
    }

    public void setValueSchema(Schema valueSchema) {
        this.valueSchema = valueSchema;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    //////// JsonIgnore ////////

    @JsonIgnore
    @Override
    public String getPrefix() {
        return prefix;
    }

    @Override
    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public String getFullName() {
        return this.prefix + this.name + (this.version > 0 ? ("." + this.version) : "");
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
