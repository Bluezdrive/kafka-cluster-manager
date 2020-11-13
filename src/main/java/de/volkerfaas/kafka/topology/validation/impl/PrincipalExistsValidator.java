package de.volkerfaas.kafka.topology.validation.impl;

import de.volkerfaas.kafka.topology.model.Domain;
import de.volkerfaas.kafka.topology.model.TopologyFile;
import de.volkerfaas.kafka.topology.validation.PrincipalExists;
import de.volkerfaas.kafka.topology.validation.ValidatorPayload;
import org.hibernate.validator.constraintvalidation.HibernateConstraintValidatorContext;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.Set;
import java.util.stream.Collectors;

public class PrincipalExistsValidator implements ConstraintValidator<PrincipalExists, String> {

    @Override
    public void initialize(PrincipalExists annotation) {

    }

    @Override
    public boolean isValid(String principal, ConstraintValidatorContext context) {
        final ValidatorPayload validatorPayload = context.unwrap(HibernateConstraintValidatorContext.class).getConstraintValidatorPayload(ValidatorPayload.class);
        final Set<String> domainPrincipals = validatorPayload.getTopologies().stream()
                .map(TopologyFile::getDomain)
                .map(Domain::getPrincipal)
                .collect(Collectors.toUnmodifiableSet());

        return domainPrincipals.contains(principal);
    }

}
