package de.volkerfaas.kafka.topology.repositories;

public class SchemaRegistryException extends Exception {

    public SchemaRegistryException(String format, Object... args) {
        super(String.format(format, args));
    }
}
