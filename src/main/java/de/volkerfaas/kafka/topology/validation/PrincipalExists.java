package de.volkerfaas.kafka.topology.validation;

import de.volkerfaas.kafka.topology.validation.impl.PrincipalExistsValidator;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target({METHOD, FIELD})
@Retention(RUNTIME)
@Constraint(validatedBy = { PrincipalExistsValidator.class })
@Documented
public @interface PrincipalExists {

    String message() default "'${validatedValue}' doesn't exist in any domain.";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

}
