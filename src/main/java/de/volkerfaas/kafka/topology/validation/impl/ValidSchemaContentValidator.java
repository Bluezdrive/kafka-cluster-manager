package de.volkerfaas.kafka.topology.validation.impl;

import de.volkerfaas.kafka.topology.model.Schema;
import de.volkerfaas.kafka.topology.validation.ValidSchemaContent;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import org.hibernate.validator.constraintvalidation.HibernateConstraintValidatorContext;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Objects;

public class ValidSchemaContentValidator implements ConstraintValidator<ValidSchemaContent, Schema> {

    @Override
    public void initialize(ValidSchemaContent annotation) {

    }

    @Override
    public boolean isValid(Schema schema, ConstraintValidatorContext context) {
        if (Objects.isNull(schema)) {
            return true;
        }
        final ValidatorPayload validatorPayload = context.unwrap(HibernateConstraintValidatorContext.class).getConstraintValidatorPayload(ValidatorPayload.class);
        if (Objects.isNull(schema.getFile())) {
            return false;
        }
        final Path path = Path.of(validatorPayload.getDirectory(), schema.getFile());
        if (!Files.exists(path)) {
            return false;
        }
        try {
            final String content = Files.readString(path);
            if (Objects.isNull(content) || content.isEmpty() || content.isBlank()) {
                return false;
            }
            final Schema.Type type = schema.getType();
            if (Objects.isNull(type)) {
                return false;
            }
            final Class<? extends SchemaProvider> schemaProviderClass = type.getSchemaProviderClass();
            final SchemaProvider schemaProvider = schemaProviderClass.getDeclaredConstructor().newInstance();
            final ParsedSchema parsedSchema = schemaProvider.parseSchema(content, Collections.emptyList()).orElse(null);

            return Objects.nonNull(parsedSchema);
        } catch (IOException | NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
    }

}
