package de.volkerfaas.kafka.topology.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import static de.volkerfaas.kafka.topology.ApplicationConfiguration.REGEX_SCHEMA_SUBJECT;

public class Schema {

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    public enum CompatibilityMode {
        BACKWARD,
        BACKWARD_TRANSITIVE,
        FORWARD,
        FORWARD_TRANSITIVE,
        FULL,
        FULL_TRANSITIVE,
        NONE
    }

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    public enum Type {
        AVRO(AvroSchemaProvider.class, "avsc"),
        PROTOBUF(ProtobufSchemaProvider.class, "proto"),
        JSON(JsonSchemaProvider.class, "json");

        private final Class<? extends SchemaProvider> schemaProviderClass;
        private final String suffix;

        Type(Class<? extends SchemaProvider> schemaProviderClass, String suffix) {
            this.schemaProviderClass = schemaProviderClass;
            this.suffix = suffix;
        }

        public Class<? extends SchemaProvider> getSchemaProviderClass() {
            return schemaProviderClass;
        }

        public String getSuffix() {
            return suffix;
        }


        @Override
        public String toString() {
            return this.name();
        }
    }

    private CompatibilityMode compatibilityMode;
    private String subject;
    private Topic topic;
    private Type type;

    public Schema() {
    }

    public Schema(String subject, Type type, CompatibilityMode compatibilityMode) {
        this.compatibilityMode = compatibilityMode;
        this.subject = subject;
        this.type = type;
    }

    public CompatibilityMode getCompatibilityMode() {
        return compatibilityMode;
    }

    public void setCompatibilityMode(CompatibilityMode compatibilityMode) {
        this.compatibilityMode = compatibilityMode;
    }

    @Pattern(
            message = "must be '(domain).(public|protected|private).(topic)-(key|value)'",
            regexp = REGEX_SCHEMA_SUBJECT
    )
    @NotBlank
    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    @NotNull
    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    //////// JsonIgnore ////////

    @JsonIgnore
    public Topic getTopic() {
        return topic;
    }

    public void setTopic(Topic topic) {
        this.topic = topic;
    }

    @Override
    public String toString() {
        return "Schema{" +
                "subject='" + subject + '\'' +
                ", compatibilityType=" + compatibilityMode +
                ", type=" + type +
                '}';
    }

}
