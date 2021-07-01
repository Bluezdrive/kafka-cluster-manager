package de.volkerfaas.kafka.topology.validation.impl;

import de.volkerfaas.kafka.topology.model.Domain;
import de.volkerfaas.kafka.topology.model.TopologyFile;
import de.volkerfaas.kafka.topology.validation.ValidTopologyFile;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.io.File;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ValidTopologyFileValidator implements ConstraintValidator<ValidTopologyFile, TopologyFile> {

    public static final String TOPOLOGY_FILE = "topology.file";
    public static final String TOPOLOGY_DOMAIN = "topology.domain";

    private Pattern pattern;

    @Override
    public void initialize(ValidTopologyFile annotation) {
        this.pattern = Pattern.compile(annotation.regex());
    }

    @Override
    public boolean isValid(final TopologyFile topologyFile, final ConstraintValidatorContext context) {
        context.disableDefaultConstraintViolation();

        boolean result = true;
        final File file = topologyFile.getFile();
        if (Objects.nonNull(file)) {
            final String fileName = file.getName();
            final Matcher matcher = pattern.matcher(fileName);
            if (!matcher.matches()) {
                context.buildConstraintViolationWithTemplate("must match " + pattern.pattern())
                        .addPropertyNode(TOPOLOGY_FILE)
                        .addConstraintViolation();
                result = false;
            } else if (matcher.groupCount() == 1) {
                final String domainName = matcher.group(1);
                final Domain domain = topologyFile.getDomain();
                if (!Objects.equals(domainName, domain.getName())) {
                    context.buildConstraintViolationWithTemplate("must have domain name " + domain.getName() + " in file name")
                            .addPropertyNode(TOPOLOGY_DOMAIN)
                            .addConstraintViolation();
                    result = false;
                }
            }
        }

        return result;
    }

}
