package de.volkerfaas.kafka.topology.listener;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;

import static com.github.stefanbirkner.systemlambda.SystemLambda.catchSystemExit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@DisplayName("When ContextRefreshedEvent is fired")
public class ContextRefreshedEventListenerTest {

    @Test
    @DisplayName("and validation fails the application should exit with code 1")
    void testValidationFailed() throws Exception {
        final ApplicationContext context = mock(ConfigurableApplicationContext.class);
        final ContextRefreshedEvent event = new ContextRefreshedEvent(context);
        final Environment environment = mock(ConfigurableEnvironment.class);
        doReturn(environment).when(context).getEnvironment();
        final ContextRefreshedEventListener listener = new ContextRefreshedEventListener();
        final int exitCode = catchSystemExit(() -> listener.onApplicationEvent(event));
        assertEquals(1, exitCode);
    }

    @Test
    @DisplayName("and validation is successful the application should continue")
    void testValidationSuccess() {
        final ApplicationContext context = mock(ConfigurableApplicationContext.class);
        final Environment environment = mock(ConfigurableEnvironment.class);
        doReturn("file:conf/").when(environment).getProperty(eq("config.resource"));
        doReturn("localhost:9092").when(environment).getProperty(eq("BOOTSTRAP_SERVER"));
        doReturn("ASX167DFJSLD").when(environment).getProperty(eq("CLUSTER_API_KEY"));
        doReturn("xldie183q823zrhvsdv").when(environment).getProperty(eq("CLUSTER_API_SECRET"));
        doReturn("https://localhost").when(environment).getProperty(eq("SCHEMA_REGISTRY_URL"));
        doReturn("DGF7384C83234R").when(environment).getProperty(eq("SCHEMA_REGISTRY_API_KEY"));
        doReturn("x759wqdeffklrv.fdfdf;").when(environment).getProperty(eq("SCHEMA_REGISTRY_API_SECRET"));
        doReturn(environment).when(context).getEnvironment();
        final ContextRefreshedEvent event = new ContextRefreshedEvent(context);
        final ContextRefreshedEventListener listener = new ContextRefreshedEventListener();
        assertThrows(AssertionError.class, () ->  catchSystemExit(() -> listener.onApplicationEvent(event)));
    }

}
