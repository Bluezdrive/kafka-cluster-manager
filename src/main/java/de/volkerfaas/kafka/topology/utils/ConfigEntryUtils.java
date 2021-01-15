package de.volkerfaas.kafka.topology.utils;

import org.apache.commons.text.WordUtils;
import org.apache.kafka.clients.admin.ConfigEntry;

import java.util.Map;

public final class ConfigEntryUtils {

    private ConfigEntryUtils() {
        throw new AssertionError("No de.volkerfaas.kafka.topology.utils.ConfigEntryUtils instances for you!");
    }

    public static String getCamelCase(final String key) {
        return WordUtils.uncapitalize(WordUtils.capitalize(key, '.').replace(".", ""));
    }

    public static String getCamelCase(final ConfigEntry configEntry) {
        return getCamelCase(configEntry.name());
    }

    public static String getCamelCase(final Map.Entry<String, String> entry) {
        return getCamelCase(entry.getKey());
    }

    public static String getDottedLowerCase(final Map.Entry<String, String> entry) {
        return entry.getKey().replaceAll("([A-Z][a-z]+)", ".$1").toLowerCase();
    }

}
