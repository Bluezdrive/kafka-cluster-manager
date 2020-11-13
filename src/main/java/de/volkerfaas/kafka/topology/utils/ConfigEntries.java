package de.volkerfaas.kafka.topology.utils;

import org.apache.commons.text.WordUtils;
import org.apache.kafka.clients.admin.ConfigEntry;

import java.util.Map;

public final class ConfigEntries {

    private ConfigEntries() {
        throw new AssertionError("No de.volkerfaas.kafka.topology.utils.ConfigEntries instances for you!");
    }

    public static String getCamelCase(String key) {
        return WordUtils.uncapitalize(WordUtils.capitalizeFully(key, '.').replace(".", ""));
    }

    public static String getCamelCase(ConfigEntry configEntry) {
        return getCamelCase(configEntry.name());
    }

    public static String getCamelCase(Map.Entry<String, String> entry) {
        return getCamelCase(entry.getKey());
    }

    public static String getDottedLowerCase(Map.Entry<String, String> entry) {
        return entry.getKey().replaceAll("([A-Z][a-z]+)", ".$1").toLowerCase();
    }

}
