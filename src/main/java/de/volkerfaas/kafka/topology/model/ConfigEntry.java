package de.volkerfaas.kafka.topology.model;

import java.util.ArrayList;
import java.util.List;

public class ConfigEntry {

    private String value;
    private final List<String> clusters;

    public ConfigEntry() {
        this.clusters = new ArrayList<>();
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public List<String> getClusters() {
        return clusters;
    }

}
