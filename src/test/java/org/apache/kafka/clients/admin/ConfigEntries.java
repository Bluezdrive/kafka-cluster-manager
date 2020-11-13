package org.apache.kafka.clients.admin;

import java.util.Collections;
import java.util.Map;

public final class ConfigEntries {

    public static ConfigEntry createDynamicTopicConfigEntry(Map.Entry<String, String> configEntry) {
        return createDynamicTopicConfigEntry(configEntry.getKey(), configEntry.getValue());
    }

    public static ConfigEntry createDynamicTopicConfigEntry(String name, String value) {
        return new ConfigEntry(name, value, ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG, false, false, Collections.emptyList());
    }

}
