package de.volkerfaas.kafka.topology.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.volkerfaas.kafka.topology.ApplicationConfiguration;
import de.volkerfaas.kafka.topology.validation.FileExists;
import de.volkerfaas.kafka.topology.validation.HasValidPartitionIncrementation;
import de.volkerfaas.kafka.topology.validation.ValidTopicConfig;

import javax.validation.Valid;
import javax.validation.constraints.*;
import java.util.*;
import java.util.stream.Collectors;

@HasValidPartitionIncrementation
public class Topic implements ConsumerAccessControl {

    public static final String NAME_REGEX = "^[a-z]+(_[a-z]+)*$";

    private String prefix;
    private String name;
    private String description;
    private final List<AccessControl> consumers;
    private int numPartitions = 6;
    private short replicationFactor = 3;
    private String keySchemaFile;
    private String valueSchemaFile;
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
    @Pattern(regexp = NAME_REGEX)
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

    @FileExists(optional = true)
    @Pattern(regexp = ApplicationConfiguration.REGEX_SCHEMA_PATH_RELATIVE)
    public String getKeySchemaFile() {
        return keySchemaFile;
    }

    public void setKeySchemaFile(String keySchemaFile) {
        this.keySchemaFile = keySchemaFile;
    }

    @NotBlank
    @FileExists
    @Pattern(regexp = ApplicationConfiguration.REGEX_SCHEMA_PATH_RELATIVE)
    public String getValueSchemaFile() {
        return valueSchemaFile;
    }

    public void setValueSchemaFile(String valueSchemaFile) {
        this.valueSchemaFile = valueSchemaFile;
    }

    @JsonIgnore
    public Set<String> getSchemaFiles() {
        Set<String> schemaFiles = new HashSet<>();
        if (Objects.nonNull(keySchemaFile)) schemaFiles.add(keySchemaFile);
        if (Objects.nonNull(valueSchemaFile)) schemaFiles.add(valueSchemaFile);

        return schemaFiles;
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
                "keySchemaFile=" + keySchemaFile + "," +
                "valueSchemaFile=" + valueSchemaFile + "," +
                "config=" + config +
                '}';
    }
}
