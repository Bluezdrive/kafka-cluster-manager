package de.volkerfaas.kafka.topology.utils;

import de.volkerfaas.kafka.topology.model.Visibility;

import java.util.List;

public final class Visibilities {

    private Visibilities() {
        throw new AssertionError("No de.volkerfaas.kafka.topology.utils.Visibilities instances for you!");
    }

    public static long countVisibilitiesOfType(List<Visibility> visibilities, Visibility.Type type) {
        return visibilities.stream().filter(visibility -> type.equals(visibility.getType())).count();
    }

}
