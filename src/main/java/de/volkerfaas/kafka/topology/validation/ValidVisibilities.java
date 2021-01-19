package de.volkerfaas.kafka.topology.validation;

import de.volkerfaas.kafka.topology.validation.impl.ValidVisibilitiesValidator;

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
@Constraint(validatedBy = { ValidVisibilitiesValidator.class })
@Documented
public @interface ValidVisibilities {

    String message() default "${validatedValue} is not a valid list of visibilities.";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};

}
