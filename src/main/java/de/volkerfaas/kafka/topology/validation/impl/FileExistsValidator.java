package de.volkerfaas.kafka.topology.validation.impl;

import de.volkerfaas.kafka.topology.ApplicationConfiguration;
import de.volkerfaas.kafka.topology.validation.FileExists;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileExistsValidator implements ConstraintValidator<FileExists, String> {

    private boolean optional;

    @Override
    public void initialize(FileExists annotation) {
        this.optional = annotation.optional();
    }

    @Override
    public boolean isValid(String pathname, ConstraintValidatorContext context) {
        if (optional && (pathname == null || pathname.isEmpty())) return true;

        final String directory = System.getProperty(ApplicationConfiguration.PROPERTY_KEY_TOPOLOGY_DIRECTORY);
        final Path path = Path.of(directory, pathname);

        return !Files.isDirectory(path) && Files.exists(path);
    }

}
