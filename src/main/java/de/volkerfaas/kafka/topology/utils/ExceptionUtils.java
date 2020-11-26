package de.volkerfaas.kafka.topology.utils;

import java.util.concurrent.Callable;

public class ExceptionUtils {

    private ExceptionUtils() {
        throw new AssertionError("No de.volkerfaas.kafka.topology.utils.ExceptionUtils instances for you!");
    }

    public static <T> T handleException(Callable<T> callable) {
        try {
            return callable.call();
        } catch(Throwable t) {
            throw new IllegalStateException(t);
        }
    }

}
