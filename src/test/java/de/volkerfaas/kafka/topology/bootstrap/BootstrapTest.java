package de.volkerfaas.kafka.topology.bootstrap;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.core.env.Environment;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@DisplayName("In the class Bootstrap")
public class BootstrapTest {

    @Nested
    @DisplayName("the method getCluster")
    class getCluster {

        @Test
        void testArgumentClusterSet() {
            Environment environment = mock(Environment.class);
            doReturn(true).when(environment).containsProperty(eq("config"));
            doReturn("development").when(environment).getProperty(eq("config"));
            final ClusterPropertiesHandler clusterPropertiesHandler = Bootstrap.init(environment).handleArgumentConfig();
            assertNotNull(clusterPropertiesHandler);
            assertEquals("development", clusterPropertiesHandler.getName());
        }

        @Test
        void testArgumentClusterNotSet() {
            Environment environment = mock(Environment.class);
            final ClusterPropertiesHandler clusterPropertiesHandler = Bootstrap.init(environment).handleArgumentConfig();
            assertNotNull(clusterPropertiesHandler);
            assertNull(clusterPropertiesHandler.getName());
        }

    }

}
