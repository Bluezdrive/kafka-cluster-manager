package de.volkerfaas.kafka.topology.validation;

import de.volkerfaas.kafka.topology.validation.impl.HasValidConfigValidator;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target({TYPE})
@Retention(RUNTIME)
@Constraint(validatedBy = { HasValidConfigValidator.class })
@Documented
public @interface HasValidConfig {

    String message() default "${validatedValue} is not a valid topic configration.";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};

}
