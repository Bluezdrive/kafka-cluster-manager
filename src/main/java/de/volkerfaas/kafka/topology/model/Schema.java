package de.volkerfaas.kafka.topology.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import de.volkerfaas.kafka.topology.validation.FileExists;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import org.apache.logging.log4j.util.Strings;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;
import java.util.Arrays;
import java.util.Objects;
import java.util.regex.Matcher;

import static de.volkerfaas.kafka.topology.ApplicationConfiguration.REGEX_SCHEMA_PATH_RELATIVE;

public class Schema {

    private static final java.util.regex.Pattern PATTERN_SCHEMA_PATH_RELATIVE = java.util.regex.Pattern.compile(REGEX_SCHEMA_PATH_RELATIVE);

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

        public static Type findBySuffix(String suffix) {
            return Arrays.stream(Type.values())
                    .filter(value -> Objects.equals(value.suffix, suffix))
                    .findFirst()
                    .orElse(null);
        }

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

    private String file;
    private CompatibilityMode compatibilityMode;
    private Type type;

    public Schema() {
    }

    public Schema(String file, Type type, CompatibilityMode compatibilityMode) {
        this.file = file;
        this.type = type;
        this.compatibilityMode = compatibilityMode;
    }

    @NotBlank
    @FileExists
    @Pattern(
            message = "must be 'events/(domain)/(domain).(public|protected|private).(topic)-(key|value).(avsc|proto|json)'",
            regexp = REGEX_SCHEMA_PATH_RELATIVE
    )
    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public CompatibilityMode getCompatibilityMode() {
        return compatibilityMode;
    }

    public void setCompatibilityMode(CompatibilityMode compatibilityMode) {
        this.compatibilityMode = compatibilityMode;
    }

    @JsonIgnore
    public String getDomainName() {
        final Matcher matcher = getMatcher();
        if (matcher == null) return null;

        return matcher.group(3);
    }

    @JsonIgnore
    public String getSubject() {
        final Matcher matcher = getMatcher();
        if (matcher == null) return null;

        return matcher.group(2);
    }

    @JsonIgnore
    public String getSuffix() {
        final Matcher matcher = getMatcher();
        if (matcher == null) return null;

        return matcher.group(7);
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "Schema{" +
                "file='" + file + '\'' +
                ", compatibilityType=" + compatibilityMode +
                ", type=" + type +
                '}';
    }

    private Matcher getMatcher() {
        if (Objects.isNull(file) || Strings.isBlank(file)) return null;
        final Matcher matcher = PATTERN_SCHEMA_PATH_RELATIVE.matcher(file);
        if (!matcher.matches() || matcher.groupCount() != 7) {
            return null;
        }
        return matcher;
    }

}
