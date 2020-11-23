package de.volkerfaas.kafka.topology.bootstrap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClusterPropertiesHandlerTest {

    @Nested
    class IsSystemPropertySet {

        @BeforeEach
        void init() {
            System.clearProperty("key");
        }

        @Test
        void testSystemPropertySet() {
            System.setProperty("key", "value");
            ClusterPropertiesHandler handler = new ClusterPropertiesHandler("test");
            final boolean result = handler.isSystemPropertySet("key", false);
            assertTrue(result);
        }

        @Test
        void testSystemPropertyNotSet() {
            ClusterPropertiesHandler handler = new ClusterPropertiesHandler("test");
            final boolean result = handler.isSystemPropertySet("key", false);
            assertFalse(result);
        }

    }

}
