package org.apache.kafka.clients.admin;

import org.javatuples.Pair;

import java.util.Collections;

public final class ConfigEntryUtils {

    private ConfigEntryUtils() {
        throw new AssertionError("No org.apache.kafka.clients.admin.ConfigEntryUtils instances for you!");
    }

    public static ConfigEntry createDynamicTopicConfigEntry(Pair<String, String> configEntry) {
        return createDynamicTopicConfigEntry(configEntry.getValue0(), configEntry.getValue1());
    }

    public static ConfigEntry createDynamicTopicConfigEntry(String name, String value) {
        return new ConfigEntry(name, value, ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG, false, false, Collections.emptyList());
    }

}
