package de.volkerfaas.kafka.topology.utils;

import org.hamcrest.Matcher;
import org.hamcrest.collection.IsMapContaining;

import java.util.Collection;
import java.util.Map;

public final class MatcherUtils {

    private MatcherUtils() {
        throw new AssertionError("No de.volkerfaas.kafka.topology.utils.MatcherUtils instances for you!");
    }

    public static <K, V> Matcher<Map<? extends K,? extends Collection<? extends V>>> hasEntryWithIterableValue(Matcher<K> key, Matcher<Iterable<? extends V>> value) {
        return IsMapContaining.hasEntry(key, value);
    }

}
