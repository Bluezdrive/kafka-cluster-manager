package de.volkerfaas.kafka.topology.bootstrap;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.core.env.ConfigurableEnvironment;

import java.io.File;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@DisplayName("In the class Bootstrap")
public class BootstrapTest {

    @Nested
    @DisplayName("the method handleArgumentCluster")
    class HandleArgumentCluster {

        @Test
        @DisplayName("should return false when no cluster is given")
        void testHandleArgumentClusterNoCluster() {
            final String[] args = new String[] { };
            final ConfigurableEnvironment environment = mock(ConfigurableEnvironment.class);
            final boolean result = Bootstrap.init(args, environment).handleArgumentCluster().result();
            assertFalse(result);
        }

        @Test
        @DisplayName("should return false when cluster is given, but file and variables don't exist")
        void testHandleArgumentClusterNoFileAndNoVariables() {
            final String[] args = new String[] { "--cluster=foo" };
            final ConfigurableEnvironment environment = mock(ConfigurableEnvironment.class);
            doReturn("classpath:conf/").when(environment).getProperty(eq("config.resource"));
            final boolean result = Bootstrap.init(args, environment).handleArgumentCluster().result();
            assertFalse(result);
        }

        @Test
        @DisplayName("should return true when cluster is given and file exists")
        void testHandleArgumentClusterFileExists() {
            final String[] args = new String[] { "--cluster=test" };
            final ConfigurableEnvironment environment = mock(ConfigurableEnvironment.class);
            doReturn("classpath:conf/").when(environment).getProperty(eq("config.resource"));
            final boolean result = Bootstrap.init(args, environment).handleArgumentCluster().result();
            assertTrue(result);
        }

        @Test
        @DisplayName("should return true when cluster is given and file doesn't exist, but variables exist")
        void testHandleArgumentClusterNoFileButVariables() {
            final String[] args = new String[] { "--cluster=foo" };
            final ConfigurableEnvironment environment = mock(ConfigurableEnvironment.class);
            doReturn("file:conf/").when(environment).getProperty(eq("config.resource"));
            doReturn(true).when(environment).containsProperty(eq("BOOTSTRAP_SERVER"));
            doReturn(true).when(environment).containsProperty(eq("CLUSTER_API_KEY"));
            doReturn(true).when(environment).containsProperty(eq("CLUSTER_API_SECRET"));
            doReturn(true).when(environment).containsProperty(eq("SCHEMA_REGISTRY_URL"));
            doReturn(true).when(environment).containsProperty(eq("SCHEMA_REGISTRY_API_KEY"));
            doReturn(true).when(environment).containsProperty(eq("SCHEMA_REGISTRY_API_SECRET"));
            final boolean result = Bootstrap.init(args, environment).handleArgumentCluster().result();
            assertTrue(result);
        }

        @Test
        @DisplayName("should return true when cluster is given, but file and variables don't exist")
        void testHandleArgumentClusterNoFileNoVariables() {
            final String[] args = new String[] { "--cluster=foo" };
            final ConfigurableEnvironment environment = mock(ConfigurableEnvironment.class);
            doReturn("file:conf/").when(environment).getProperty(eq("config.resource"));
            final boolean result = Bootstrap.init(args, environment).handleArgumentCluster().result();
            assertFalse(result);
        }

    }

    @Nested
    @DisplayName("the method handleArgumentDirectory")
    class HandleArgumentDirectory {

        @Test
        @DisplayName("should return true when the given directory exists")
        void testHandleArgumentDirectory() {
            final URL resource = getClass().getClassLoader().getResource("topology-de.volkerfaas.arc.yaml");
            assertNotNull(resource);
            final String directory = new File(resource.getPath()).getParent();
            final String[] args = new String[] { "--directory=" + directory };
            final ConfigurableEnvironment environment = mock(ConfigurableEnvironment.class);
            final boolean result = Bootstrap.init(args, environment).handleArgumentDirectory().result();
            assertTrue(result);
        }

        @Test
        @DisplayName("should return false when the given directory doesn't exist")
        void testHandleArgumentDirectoryNotExists() {
            final String[] args = new String[] { "--directory=foo" };
            final ConfigurableEnvironment environment = mock(ConfigurableEnvironment.class);
            final boolean result = Bootstrap.init(args, environment).handleArgumentDirectory().result();
            assertFalse(result);
        }

    }

    @Nested
    @DisplayName("the method validateArguments")
    class ValidateArguments {

        @ParameterizedTest
        @ValueSource(strings = {
                "--help",
                "--directory",
                "--domain",
                "--allow-delete-acl",
                "--allow-delete-topics",
                "--dry-run"
        })
        void testValidateOptionalArguments(String arg) {
            final String[] args = new String[] { arg, "--cluster" };
            final ConfigurableEnvironment environment = mock(ConfigurableEnvironment.class);
            final boolean result = Bootstrap.init(args, environment).validateArguments().result();
            assertTrue(result);
        }

        @Test
        void testValidateClusterArgumentNotSet() {
            final String[] args = new String[] {};
            final ConfigurableEnvironment environment = mock(ConfigurableEnvironment.class);
            final boolean result = Bootstrap.init(args, environment).validateArguments().result();
            assertFalse(result);
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "--dir",
                "--dom",
                "--conf",
                "--allowdelete-acl",
                "--allow-deletetopics",
                "--dryrun",
                "--rest"
        })
        void testValidateArgumentsNotAllowed(String arg) {
            final String[] args = new String[] { arg, "--cluster" };
            final ConfigurableEnvironment environment = mock(ConfigurableEnvironment.class);
            final boolean result = Bootstrap.init(args, environment).validateArguments().result();
            assertFalse(result);
        }

    }

}
