package de.volkerfaas.kafka.topology.utils;

import org.apache.logging.log4j.util.Strings;
import org.springframework.boot.ApplicationArguments;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class ApplicationArgumentsUtils {

    private ApplicationArgumentsUtils() {
        throw new AssertionError("No de.volkerfaas.kafka.topology.utils.ApplicationArgumentsUtils instances for you!");
    }

    public static String firstStringValueOf(ApplicationArguments args, String name, String defaultValue) {
        if (!args.containsOption(name)) return defaultValue;

        return args.getOptionValues(name).stream()
                .findFirst()
                .filter(Strings::isNotEmpty)
                .filter(Strings::isNotBlank)
                .orElse(defaultValue);
    }

    public static List<String> stringValuesOf(ApplicationArguments args, String name) {
        final List<String> values = args.getOptionValues(name);
        if (Objects.isNull(values) || values.isEmpty()) {
            return Collections.emptyList();
        }
        return values.stream()
                .filter(Strings::isNotEmpty)
                .filter(Strings::isNotBlank)
                .collect(Collectors.toList());
    }

}
