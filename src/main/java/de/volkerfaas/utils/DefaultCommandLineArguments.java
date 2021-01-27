package de.volkerfaas.utils;

import org.apache.commons.lang3.StringUtils;
import org.springframework.core.env.SimpleCommandLinePropertySource;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class DefaultCommandLineArguments implements CommandLineArguments {

    private final Source source;

    public DefaultCommandLineArguments(String... args) {
        this.source = new Source(args);
    }

    @Override
    public Set<String> getOptionNames() {
        return Set.of(this.source.getPropertyNames());
    }

    @Override
    public boolean containsOption(CommandLineArgument argument) {
        final String name = argument.getName();
        return this.source.containsProperty(name);
    }

    @Override
    public List<String> getOptionValues(CommandLineArgument argument) {
        return source.getOptionValues(argument);
    }

    @Override
    public String getOptionValue(CommandLineArgument argument) {
        final List<String> values = getOptionValues(argument);
        if (values.isEmpty()) {
            return argument.getDefaultValue();
        }
        return values.stream()
                .findFirst()
                .orElse(argument.getDefaultValue());
    }

    /*
    @Override
    public String getOptionValue(CommandLineArgument argument) {
        return getOptionValue(argument, null);
    }

    @Override
    public String getOptionValue(CommandLineArgument argument, String defaultValue) {
        final List<String> values = getOptionValues(argument);
        if (values.isEmpty()) {
            return defaultValue;
        }
        return values.stream()
                .findFirst()
                .orElse(defaultValue);
    }
    */

    @Override
    public String getRequiredOptionValue(CommandLineArgument argument) {
        final String value = getOptionValue(argument);
        if (Objects.isNull(value)) {
            throw new IllegalArgumentException("Option '" + argument.getName() + "' must be set.");
        }
        return value;
    }

    private static class Source extends SimpleCommandLinePropertySource {

        Source(String[] args) {
            super(args);
        }

        public List<String> getOptionValues(CommandLineArgument argument) {
            final String name = argument.getName();
            if (name.isEmpty()) {
                return Collections.emptyList();
            }
            final List<String> values = super.getOptionValues(name);
            if (Objects.isNull(values) || values.isEmpty()) {
                return Collections.emptyList();
            }
            return values.stream()
                    .filter(Objects::nonNull)
                    .filter(StringUtils::isNotEmpty)
                    .filter(StringUtils::isNotBlank)
                    .collect(Collectors.toUnmodifiableList());
        }

    }

}
