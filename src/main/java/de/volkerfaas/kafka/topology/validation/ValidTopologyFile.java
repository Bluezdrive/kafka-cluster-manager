package de.volkerfaas.kafka.topology.validation;

import de.volkerfaas.kafka.topology.ApplicationConfiguration;
import de.volkerfaas.kafka.topology.validation.impl.ValidTopologyFileValidator;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target({TYPE, ANNOTATION_TYPE})
@Retention(RUNTIME)
@Constraint(validatedBy = { ValidTopologyFileValidator.class })
@Documented
public @interface ValidTopologyFile {

    String message() default "${validatedValue} is not a valid topology file.";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};
    String regex() default ApplicationConfiguration.REGEX_TOPOLOGY_FILENAME;

}
