package de.volkerfaas.kafka.topology.bootstrap;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("In the class Bootstrap")
public class BootstrapTest {

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
            final boolean result = Bootstrap.init(args).handleArgumentDirectory().result();
            assertTrue(result);
        }

        @Test
        @DisplayName("should return false when the given directory doesn't exist")
        void testHandleArgumentDirectoryNotExists() {
            final String[] args = new String[] { "--directory=foo" };
            final boolean result = Bootstrap.init(args).handleArgumentDirectory().result();
            assertFalse(result);
        }

    }

    @Nested
    @DisplayName("the method validateArguments")
    class ValidateArguments {

        @ParameterizedTest
        @ValueSource(strings = {
                "--directory",
                "--domain",
                "--allow-delete-acl",
                "--allow-delete-topics",
                "--dry-run"
        })
        void testValidateOptionalArguments(String arg) {
            final String[] args = new String[] { "deploy", arg, "--cluster" };
            final boolean result = Bootstrap.init(args).validateArguments().result();
            assertTrue(result);
        }

        @Test
        void testValidateClusterArgumentNotSet() {
            final String[] args = new String[] { "deploy" };
            final boolean result = Bootstrap.init(args).validateArguments().result();
            assertTrue(result);
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
            final String[] args = new String[] { "deploy", arg, "--cluster" };
            final boolean result = Bootstrap.init(args).validateArguments().result();
            assertFalse(result);
        }

    }

}
