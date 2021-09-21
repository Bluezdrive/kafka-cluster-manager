package de.volkerfaas.kafka.topology.utils;

import de.volkerfaas.kafka.topology.model.Schema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static de.volkerfaas.kafka.topology.ApplicationConfiguration.EVENTS_DIRECTORY;
import static de.volkerfaas.kafka.topology.ApplicationConfiguration.REGEX_SCHEMA_SUBJECT;

public final class SchemaUtils {

    private static final Pattern PATTERN_SCHEMA_SUBJECT = Pattern.compile(REGEX_SCHEMA_SUBJECT);

    private SchemaUtils() {
        throw new AssertionError("No de.volkerfaas.kafka.topology.utils.SchemaUtils instances for you!");
    }

    public static Path getSchemaPath(Schema schema, String directory) throws IOException {
        if (Objects.isNull(schema)) {
            return null;
        }

        final String subject = schema.getSubject();
        final Schema.Type type = schema.getType();
        if (Objects.isNull(subject) || Objects.isNull(type)) {
            return null;
        }

        final Matcher matcher = PATTERN_SCHEMA_SUBJECT.matcher(subject);
        final boolean matches = matcher.matches();
        if (!matches) {
            return null;
        }

        final String domainName = matcher.group(1);
        final Path schemaDirectoryPath = Path.of(directory, EVENTS_DIRECTORY, domainName);
        Files.createDirectories(schemaDirectoryPath);

        return Path.of(schemaDirectoryPath.toString(), subject + "." + type.getSuffix());
    }

}
