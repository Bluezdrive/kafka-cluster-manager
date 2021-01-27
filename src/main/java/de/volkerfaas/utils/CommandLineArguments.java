package de.volkerfaas.utils;

import java.util.List;
import java.util.Set;

public interface CommandLineArguments {
    Set<String> getOptionNames();

    boolean containsOption(CommandLineArgument argument);

    List<String> getOptionValues(CommandLineArgument argument);

    String getOptionValue(CommandLineArgument argument);

    //String getOptionValue(CommandLineArgument argument, String defaultValue);

    String getRequiredOptionValue(CommandLineArgument argument);
}
