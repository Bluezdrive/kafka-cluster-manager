package de.volkerfaas.kafka.topology.validation;

import de.volkerfaas.kafka.topology.validation.impl.ValidSchemaSubjectValidator;

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
@Constraint(validatedBy = { ValidSchemaSubjectValidator.class })
@Documented
public @interface ValidSchemaSubject {

    String message() default "'${validatedValue.subject}' doesn't match with topic name '${validatedValue.topic.fullName}'.";
    String type() default "value";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};

}
