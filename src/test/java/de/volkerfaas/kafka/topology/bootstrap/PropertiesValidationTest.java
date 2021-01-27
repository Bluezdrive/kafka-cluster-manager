package de.volkerfaas.kafka.topology.bootstrap;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.core.env.ConfigurableEnvironment;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@DisplayName("In the class PropertiesValidation")
public class PropertiesValidationTest {

    @Nested
    @DisplayName("the method validateProperties")
    class ValidateProperties {

        @Test
        @DisplayName("should return true when all variables are valid")
        void testValid() {
            final ConfigurableEnvironment environment = mock(ConfigurableEnvironment.class);
            doReturn("localhost:9092").when(environment).getProperty(eq("BOOTSTRAP_SERVER"));
            doReturn("ASX167DFJSLD").when(environment).getProperty(eq("CLUSTER_API_KEY"));
            doReturn("xldie183q823zrhvsdv").when(environment).getProperty(eq("CLUSTER_API_SECRET"));
            doReturn("https://localhost").when(environment).getProperty(eq("SCHEMA_REGISTRY_URL"));
            doReturn("DGF7384C83234R").when(environment).getProperty(eq("SCHEMA_REGISTRY_API_KEY"));
            doReturn("x759wqdeffklrv.fdfdf;").when(environment).getProperty(eq("SCHEMA_REGISTRY_API_SECRET"));
            final boolean result = PropertiesValidation.init(environment).validateProperties().result();
            assertTrue(result);
        }

        @ParameterizedTest
        @NullAndEmptySource
        @ValueSource(strings = { "localhost", "http://localhost", "https://localhost", "x" })
        @DisplayName("should return false when BOOTSTRAP_SERVER is not valid")
        void testInvalidBootstrapServer(String bootstrapServer) {
            final ConfigurableEnvironment environment = mock(ConfigurableEnvironment.class);
            doReturn(bootstrapServer).when(environment).getProperty(eq("BOOTSTRAP_SERVER"));
            doReturn("ASX167DFJSLD").when(environment).getProperty(eq("CLUSTER_API_KEY"));
            doReturn("xldie183q823zrhvsdv").when(environment).getProperty(eq("CLUSTER_API_SECRET"));
            doReturn("https://localhost").when(environment).getProperty(eq("SCHEMA_REGISTRY_URL"));
            doReturn("DGF7384C83234R").when(environment).getProperty(eq("SCHEMA_REGISTRY_API_KEY"));
            doReturn("x759wqdeffklrv.fdfdf;").when(environment).getProperty(eq("SCHEMA_REGISTRY_API_SECRET"));
            final boolean result = PropertiesValidation.init(environment).validateProperties().result();
            assertFalse(result);
        }

        @ParameterizedTest
        @NullAndEmptySource
        @DisplayName("should return false when CLUSTER_API_KEY is null or empty")
        void testInvalidClusterApiKey(String clusterApiKey) {
            final ConfigurableEnvironment environment = mock(ConfigurableEnvironment.class);
            doReturn("localhost").when(environment).getProperty(eq("BOOTSTRAP_SERVER"));
            doReturn(clusterApiKey).when(environment).getProperty(eq("CLUSTER_API_KEY"));
            doReturn("xldie183q823zrhvsdv").when(environment).getProperty(eq("CLUSTER_API_SECRET"));
            doReturn("https://localhost").when(environment).getProperty(eq("SCHEMA_REGISTRY_URL"));
            doReturn("DGF7384C83234R").when(environment).getProperty(eq("SCHEMA_REGISTRY_API_KEY"));
            doReturn("x759wqdeffklrv.fdfdf;").when(environment).getProperty(eq("SCHEMA_REGISTRY_API_SECRET"));
            final boolean result = PropertiesValidation.init(environment).validateProperties().result();
            assertFalse(result);
        }

        @ParameterizedTest
        @NullAndEmptySource
        @DisplayName("should return false when CLUSTER_API_SECRET is null or empty")
        void testInvalidClusterApiSecret(String clusterApiSecret) {
            final ConfigurableEnvironment environment = mock(ConfigurableEnvironment.class);
            doReturn("localhost").when(environment).getProperty(eq("BOOTSTRAP_SERVER"));
            doReturn("ASX167DFJSLD").when(environment).getProperty(eq("CLUSTER_API_KEY"));
            doReturn(clusterApiSecret).when(environment).getProperty(eq("CLUSTER_API_SECRET"));
            doReturn("https://localhost").when(environment).getProperty(eq("SCHEMA_REGISTRY_URL"));
            doReturn("DGF7384C83234R").when(environment).getProperty(eq("SCHEMA_REGISTRY_API_KEY"));
            doReturn("x759wqdeffklrv.fdfdf;").when(environment).getProperty(eq("SCHEMA_REGISTRY_API_SECRET"));
            final boolean result = PropertiesValidation.init(environment).validateProperties().result();
            assertFalse(result);
        }

        @ParameterizedTest
        @NullAndEmptySource
        @ValueSource(strings = { "localhost", "http://localhost", "https://localhost:9002", "x" })
        @DisplayName("should return false when SCHEMA_REGISTRY_URL is null or empty")
        void testInvalidSchemaRegistryUrl(String schemaRegistryUrl) {
            final ConfigurableEnvironment environment = mock(ConfigurableEnvironment.class);
            doReturn("localhost").when(environment).getProperty(eq("BOOTSTRAP_SERVER"));
            doReturn("ASX167DFJSLD").when(environment).getProperty(eq("CLUSTER_API_KEY"));
            doReturn("xldie183q823zrhvsdv").when(environment).getProperty(eq("CLUSTER_API_SECRET"));
            doReturn(schemaRegistryUrl).when(environment).getProperty(eq("SCHEMA_REGISTRY_URL"));
            doReturn("DGF7384C83234R").when(environment).getProperty(eq("SCHEMA_REGISTRY_API_KEY"));
            doReturn("x759wqdeffklrv.fdfdf;").when(environment).getProperty(eq("SCHEMA_REGISTRY_API_SECRET"));
            final boolean result = PropertiesValidation.init(environment).validateProperties().result();
            assertFalse(result);
        }

        @ParameterizedTest
        @NullAndEmptySource
        @DisplayName("should return false when SCHEMA_REGISTRY_API_KEY is null or empty")
        void testInvalidSchemaRegistryApiKey(String schemaRegistryApiKey) {
            final ConfigurableEnvironment environment = mock(ConfigurableEnvironment.class);
            doReturn("localhost").when(environment).getProperty(eq("BOOTSTRAP_SERVER"));
            doReturn("ASX167DFJSLD").when(environment).getProperty(eq("CLUSTER_API_KEY"));
            doReturn("xldie183q823zrhvsdv").when(environment).getProperty(eq("CLUSTER_API_SECRET"));
            doReturn("https://localhost").when(environment).getProperty(eq("SCHEMA_REGISTRY_URL"));
            doReturn(schemaRegistryApiKey).when(environment).getProperty(eq("SCHEMA_REGISTRY_API_KEY"));
            doReturn("x759wqdeffklrv.fdfdf;").when(environment).getProperty(eq("SCHEMA_REGISTRY_API_SECRET"));
            final boolean result = PropertiesValidation.init(environment).validateProperties().result();
            assertFalse(result);
        }

        @ParameterizedTest
        @NullAndEmptySource
        @DisplayName("should return false when SCHEMA_REGISTRY_API_SECRET is null or empty")
        void testInvalidSchemaRegistryApiSecret(String schemaRegistryApiSecret) {
            final ConfigurableEnvironment environment = mock(ConfigurableEnvironment.class);
            doReturn("localhost").when(environment).getProperty(eq("BOOTSTRAP_SERVER"));
            doReturn("ASX167DFJSLD").when(environment).getProperty(eq("CLUSTER_API_KEY"));
            doReturn("xldie183q823zrhvsdv").when(environment).getProperty(eq("CLUSTER_API_SECRET"));
            doReturn("https://localhost").when(environment).getProperty(eq("SCHEMA_REGISTRY_URL"));
            doReturn("DGF7384C83234R").when(environment).getProperty(eq("SCHEMA_REGISTRY_API_KEY"));
            doReturn(schemaRegistryApiSecret).when(environment).getProperty(eq("SCHEMA_REGISTRY_API_SECRET"));
            final boolean result = PropertiesValidation.init(environment).validateProperties().result();
            assertFalse(result);
        }

    }

}
