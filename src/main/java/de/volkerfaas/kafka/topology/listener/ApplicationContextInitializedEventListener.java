package de.volkerfaas.kafka.topology.listener;

import de.volkerfaas.kafka.topology.bootstrap.Bootstrap;
import de.volkerfaas.kafka.topology.bootstrap.Help;
import org.jetbrains.annotations.NotNull;
import org.springframework.boot.context.event.ApplicationContextInitializedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;

public class ApplicationContextInitializedEventListener implements ApplicationListener<ApplicationContextInitializedEvent> {

    @Override
    public void onApplicationEvent(@NotNull final ApplicationContextInitializedEvent event) {
        final String[] args = event.getArgs();
        final boolean helpResult = Help.init(args).handleArgumentHelp().result();
        if (helpResult) {
            System.exit(0);
        }

        final ConfigurableApplicationContext context = event.getApplicationContext();
        final ConfigurableEnvironment environment = context.getEnvironment();
        final boolean bootstrapResult = Bootstrap.init(args, environment)
                .validateArguments()
                .handleArgumentDirectory()
                .handleArgumentCluster()
                .result();
        if (!bootstrapResult) {
            System.exit(1);
        }
    }

}
