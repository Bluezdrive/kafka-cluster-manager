package de.volkerfaas.utils;

import java.util.List;
import java.util.Set;

public interface CommandLineArguments {
    Set<String> getPropertyNames();

    List<String> getOptions();

    boolean containsOption(CommandLineOption option);

    boolean containsProperty(CommandLineProperty argument);

    List<String> getPropertyValues(CommandLineProperty argument);

    String getPropertyValue(CommandLineProperty argument);

    //String getOptionValue(CommandLineArgument argument, String defaultValue);

    String getRequiredPropertyValue(CommandLineProperty argument);
}
