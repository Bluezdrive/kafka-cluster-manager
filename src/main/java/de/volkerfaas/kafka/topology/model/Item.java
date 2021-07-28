package de.volkerfaas.kafka.topology.model;

public interface Item {

    String getFullName();
    String getPrefix();
    void setPrefix(String prefix);

}
