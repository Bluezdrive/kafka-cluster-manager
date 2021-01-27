package de.volkerfaas.kafka.topology.listener;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.event.ApplicationContextInitializedEvent;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;

import java.io.File;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static uk.org.webcompere.systemstubs.SystemStubs.catchSystemExit;

@DisplayName("When ApplicationContextInitializedEvent is fired")
public class ApplicationContextInitializedEventListenerTest {

    @Test
    @DisplayName("and option help is given the application should exit with code 0")
    void testHelp() throws Exception {
        final String[] args = new String[] { "--help" };
        final SpringApplication application = mock(SpringApplication.class);
        final ConfigurableApplicationContext context = mock(ConfigurableApplicationContext.class);
        final ConfigurableEnvironment environment = mock(ConfigurableEnvironment.class);
        doReturn(environment).when(context).getEnvironment();
        final ApplicationContextInitializedEventListener listener = new ApplicationContextInitializedEventListener();
        final ApplicationContextInitializedEvent event = new ApplicationContextInitializedEvent(application, args, context);
        final int exitCode = catchSystemExit(() -> listener.onApplicationEvent(event));
        assertEquals(0, exitCode);
    }

    @Test
    @DisplayName("and an error occurs during startup the application should exit with code 1")
    void testFail() throws Exception {
        final String[] args = new String[] { "--cluster=fake" };
        final SpringApplication application = mock(SpringApplication.class);
        final ConfigurableApplicationContext context = mock(ConfigurableApplicationContext.class);
        final ConfigurableEnvironment environment = mock(ConfigurableEnvironment.class);
        doReturn(environment).when(context).getEnvironment();
        final ApplicationContextInitializedEventListener listener = new ApplicationContextInitializedEventListener();
        final ApplicationContextInitializedEvent event = new ApplicationContextInitializedEvent(application, args, context);
        final int exitCode = catchSystemExit(() -> listener.onApplicationEvent(event));
        assertEquals(1, exitCode);
    }

    @Test
    @DisplayName("and an error occurs during startup the application should continue")
    void testSuccess() {
        final URL resource = getClass().getClassLoader().getResource("topology-de.volkerfaas.arc.yaml");
        assertNotNull(resource);
        final String directory = new File(resource.getPath()).getParent();
        final String[] args = new String[] { "--cluster=test", "--directory=" + directory };
        final SpringApplication application = mock(SpringApplication.class);
        final ConfigurableApplicationContext context = mock(ConfigurableApplicationContext.class);
        final ConfigurableEnvironment environment = mock(ConfigurableEnvironment.class);
        doReturn("file:conf/").when(environment).getProperty(eq("config.resource"));
        doReturn(true).when(environment).containsProperty(eq("BOOTSTRAP_SERVER"));
        doReturn(true).when(environment).containsProperty(eq("CLUSTER_API_KEY"));
        doReturn(true).when(environment).containsProperty(eq("CLUSTER_API_SECRET"));
        doReturn(true).when(environment).containsProperty(eq("SCHEMA_REGISTRY_URL"));
        doReturn(true).when(environment).containsProperty(eq("SCHEMA_REGISTRY_API_KEY"));
        doReturn(true).when(environment).containsProperty(eq("SCHEMA_REGISTRY_API_SECRET"));
        doReturn(environment).when(context).getEnvironment();
        final ApplicationContextInitializedEventListener listener = new ApplicationContextInitializedEventListener();
        final ApplicationContextInitializedEvent event = new ApplicationContextInitializedEvent(application, args, context);
        assertThrows(AssertionError.class, () -> catchSystemExit(() -> listener.onApplicationEvent(event)));
    }

}
