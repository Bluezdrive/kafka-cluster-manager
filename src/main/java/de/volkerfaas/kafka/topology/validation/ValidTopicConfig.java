package de.volkerfaas.kafka.topology.validation;

import de.volkerfaas.kafka.topology.validation.impl.ValidTopicConfigValidator;

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
@Constraint(validatedBy = { ValidTopicConfigValidator.class })
@Documented
public @interface ValidTopicConfig {
    String message() default "${validatedValue} is not a valid topic configration.";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
