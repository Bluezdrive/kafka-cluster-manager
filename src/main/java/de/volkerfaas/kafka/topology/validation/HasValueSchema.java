package de.volkerfaas.kafka.topology.validation;

import de.volkerfaas.kafka.topology.model.Visibility;
import de.volkerfaas.kafka.topology.validation.impl.HasValueSchemaValidator;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static de.volkerfaas.kafka.topology.model.Visibility.Type.*;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target({TYPE})
@Retention(RUNTIME)
@Constraint(validatedBy = { HasValueSchemaValidator.class })
@Documented
public @interface HasValueSchema {

    Visibility.Type[] visibilities() default { PUBLIC, PROTECTED, PRIVATE };
    String message() default "misses value schemas.";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};

}
