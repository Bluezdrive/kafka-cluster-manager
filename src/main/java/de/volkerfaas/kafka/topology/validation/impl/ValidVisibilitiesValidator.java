package de.volkerfaas.kafka.topology.validation.impl;

import de.volkerfaas.kafka.topology.model.Visibility;
import de.volkerfaas.kafka.topology.utils.VisibilityUtils;
import de.volkerfaas.kafka.topology.validation.ValidVisibilities;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.List;

public class ValidVisibilitiesValidator implements ConstraintValidator<ValidVisibilities, List<Visibility>> {

    private static final String VISIBILITY = "visibility";

    @Override
    public void initialize(ValidVisibilities annotation) {

    }

    @Override
    public boolean isValid(final List<Visibility> visibilities, final ConstraintValidatorContext context) {
        boolean result = true;
        context.disableDefaultConstraintViolation();

        for (final Visibility.Type type : Visibility.Type.values()) {
            final long count = VisibilityUtils.countVisibilitiesOfType(visibilities, type);
            if (count > 1) {
                context.buildConstraintViolationWithTemplate("must only contain zero or one element of type '" + type.getValue() + "'")
                        .addPropertyNode(VISIBILITY)
                        .addConstraintViolation();
                result = false;
            }
        }

        return result;
    }



}
