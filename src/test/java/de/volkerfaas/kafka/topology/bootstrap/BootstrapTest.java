package de.volkerfaas.kafka.topology.bootstrap;

import com.github.stefanbirkner.systemlambda.SystemLambda;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.SimpleCommandLinePropertySource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@DisplayName("In the class Bootstrap")
public class BootstrapTest {

    @Nested
    @DisplayName("the method handleArgumentConfig")
    class HandleArgumentConfig {

        @Test
        void testHandleArgumentConfig() {
            final ConfigurableEnvironment environment = mock(ConfigurableEnvironment.class);
            doReturn(true).when(environment).containsProperty(eq("config"));
            doReturn("development").when(environment).getProperty(eq("config"));
            final ClusterPropertiesHandler clusterPropertiesHandler = Bootstrap.init(environment).handleArgumentConfig();
            assertNotNull(clusterPropertiesHandler);
            assertEquals("development", clusterPropertiesHandler.getFileName());
        }

        @Test
        void testHandleArgumentConfigNotSet() {
            final ConfigurableEnvironment environment = mock(ConfigurableEnvironment.class);
            final ClusterPropertiesHandler clusterPropertiesHandler = Bootstrap.init(environment).handleArgumentConfig();
            assertNotNull(clusterPropertiesHandler);
            assertNull(clusterPropertiesHandler.getFileName());
        }

    }

    @Nested
    @DisplayName("the method validateArguments")
    class ValidateArguments {

        @ParameterizedTest
        @ValueSource(strings = {
                "help",
                "directory",
                "domain",
                "config",
                "allow-delete-acl",
                "allow-delete-topics",
                "dry-run",
                "restore"
        })
        void testValidateArguments(String arg) {
            final String[] args = new String[] { arg };
            final MutablePropertySources mutablePropertySources = new MutablePropertySources();
            final SimpleCommandLinePropertySource commandLinePropertySource = mock(SimpleCommandLinePropertySource.class);
            doReturn(args).when(commandLinePropertySource).getPropertyNames();
            mutablePropertySources.addLast(commandLinePropertySource);
            final ConfigurableEnvironment environment = mock(ConfigurableEnvironment.class);
            doReturn(mutablePropertySources).when(environment).getPropertySources();
            doReturn("development").when(environment).getProperty(eq("config"));
            Bootstrap.init(environment).validateArguments();
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "dir",
                "dom",
                "conf",
                "allowdelete-acl",
                "allow-deletetopics",
                "dryrun",
                "rest"
        })
        void testValidateArgumentsFails(String arg) throws Exception {
            final String[] args = new String[] { arg };
            final MutablePropertySources mutablePropertySources = new MutablePropertySources();
            final SimpleCommandLinePropertySource commandLinePropertySource = mock(SimpleCommandLinePropertySource.class);
            doReturn("commandLineArgs").when(commandLinePropertySource).getName();
            doReturn(args).when(commandLinePropertySource).getPropertyNames();
            mutablePropertySources.addLast(commandLinePropertySource);
            final ConfigurableEnvironment environment = mock(ConfigurableEnvironment.class);
            doReturn(mutablePropertySources).when(environment).getPropertySources();
            doReturn("development").when(environment).getProperty(eq("config"));
            int status = SystemLambda.catchSystemExit(() -> Bootstrap.init(environment).validateArguments());
            assertEquals(1, status);
        }

    }

}
