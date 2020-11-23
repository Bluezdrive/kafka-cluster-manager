package de.volkerfaas.kafka.topology.validation.impl;

import de.volkerfaas.kafka.topology.validation.FileExists;
import org.hibernate.validator.constraintvalidation.HibernateConstraintValidatorContext;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public class FileExistsValidator implements ConstraintValidator<FileExists, String> {


    @Override
    public void initialize(FileExists annotation) {

    }

    @Override
    public boolean isValid(final String pathname, final ConstraintValidatorContext context) {
        if (Objects.isNull(pathname)) {
            return false;
        }

        final ValidatorPayload validatorPayload = context.unwrap(HibernateConstraintValidatorContext.class).getConstraintValidatorPayload(ValidatorPayload.class);
        final Path path = Path.of(validatorPayload.getDirectory(), pathname);

        return !Files.isDirectory(path) && Files.exists(path);
    }

}
