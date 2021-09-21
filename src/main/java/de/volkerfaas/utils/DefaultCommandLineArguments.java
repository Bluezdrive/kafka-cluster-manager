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
    public Set<String> getPropertyNames() {
        return Set.of(this.source.getPropertyNames());
    }

    @Override
    public List<String> getOptions() {
        return this.source.getNonOptionArgs();
    }

    @Override
    public boolean containsOption(CommandLineOption option) {
        final String name = option.getName();
        return this.source.getNonOptionArgs().contains(name);
    }

    @Override
    public boolean containsProperty(CommandLineProperty property) {
        final String name = property.getName();
        return this.source.containsProperty(name);
    }

    @Override
    public List<String> getPropertyValues(CommandLineProperty argument) {
        return source.getOptionValues(argument);
    }

    @Override
    public String getPropertyValue(CommandLineProperty argument) {
        final List<String> values = getPropertyValues(argument);
        if (values.isEmpty()) {
            return argument.getDefaultValue();
        }
        return values.stream()
                .findFirst()
                .orElse(argument.getDefaultValue());
    }

    @Override
    public String getRequiredPropertyValue(CommandLineProperty argument) {
        final String value = getPropertyValue(argument);
        if (Objects.isNull(value)) {
            throw new IllegalArgumentException("Option '" + argument.getName() + "' must be set.");
        }
        return value;
    }

    private static class Source extends SimpleCommandLinePropertySource {

        Source(String[] args) {
            super(args);
        }

        public List<String> getNonOptionArgs() {
            return super.getNonOptionArgs();
        }

        public boolean containsOption(String name) {
            return super.containsOption(name);
        }

        public List<String> getOptionValues(CommandLineProperty argument) {
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
