package de.volkerfaas.kafka.topology.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConfigFile {

    private final Map<String, List<ConfigEntry>> entries;


    public ConfigFile() {
        this.entries = new HashMap<>();
    }

    @JsonProperty
    public Map<String, List<ConfigEntry>> getEntries() {
        return entries;
    }
}
