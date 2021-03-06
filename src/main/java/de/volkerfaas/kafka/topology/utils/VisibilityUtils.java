package de.volkerfaas.kafka.topology.utils;

import de.volkerfaas.kafka.topology.model.Visibility;

import java.util.List;

public final class VisibilityUtils {

    private VisibilityUtils() {
        throw new AssertionError("No de.volkerfaas.kafka.topology.utils.VisibilityUtils instances for you!");
    }

    public static long countVisibilitiesOfType(final List<Visibility> visibilities, final Visibility.Type type) {
        return visibilities.stream().filter(visibility -> type.equals(visibility.getType())).count();
    }

}
