package de.volkerfaas.kafka.topology.validation.impl;

import de.volkerfaas.kafka.topology.model.Schema;
import de.volkerfaas.kafka.topology.utils.SchemaUtils;
import de.volkerfaas.kafka.topology.validation.SchemaFileExists;
import org.hibernate.validator.constraintvalidation.HibernateConstraintValidatorContext;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public class SchemaFileExistsValidator implements ConstraintValidator<SchemaFileExists, Schema> {


    @Override
    public void initialize(SchemaFileExists annotation) {

    }

    @Override
    public boolean isValid(final Schema schema, final ConstraintValidatorContext context) {
        if (Objects.isNull(schema) || Objects.isNull(schema.getSubject())) {
            return true;
        }

        final ValidatorPayload validatorPayload = context.unwrap(HibernateConstraintValidatorContext.class).getConstraintValidatorPayload(ValidatorPayload.class);
        try {
            final Path path = SchemaUtils.getSchemaPath(schema, validatorPayload.getDirectory());
            if (Objects.isNull(path)) {
                return false;
            }
            return !Files.isDirectory(path) && Files.exists(path);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

}
