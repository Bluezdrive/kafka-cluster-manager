package de.volkerfaas.kafka.topology.validation;

import de.volkerfaas.kafka.topology.validation.impl.SchemaFileExistsValidator;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target({FIELD, METHOD})
@Retention(RUNTIME)
@Constraint(validatedBy = { SchemaFileExistsValidator.class })
@Documented
public @interface SchemaFileExists {

    String message() default "'${validatedValue.subject}' doesn't exist.";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};

}
