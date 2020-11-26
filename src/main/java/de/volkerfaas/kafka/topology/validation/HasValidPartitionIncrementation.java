package de.volkerfaas.kafka.topology.validation;

import de.volkerfaas.kafka.topology.validation.impl.HasValidPartitionIncrementationValidator;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target(TYPE)
@Retention(RUNTIME)
@Constraint(validatedBy = { HasValidPartitionIncrementationValidator.class })
@Documented
public @interface HasValidPartitionIncrementation {
    String message() default "'${validatedValue.fullName}' has not a valid partition incrementation.";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
