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

    public static String valueOf(final ApplicationArguments args, final String name, final String defaultValue) {
        if (Objects.isNull(args) || !args.containsOption(name)) return defaultValue;
        return args.getOptionValues(name).stream()
                .filter(Objects::nonNull)
                .filter(Strings::isNotEmpty)
                .filter(Strings::isNotBlank)
                .findFirst()
                .orElse(defaultValue);
    }

    public static String requiredValueOf(final ApplicationArguments args, final String name) {
        return args.getOptionValues(name).stream()
                .filter(Objects::nonNull)
                .filter(Strings::isNotEmpty)
                .filter(Strings::isNotBlank)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Argument '" + name + "' not set."));
    }

    public static List<String> valuesOf(final ApplicationArguments args, final String name) {
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
