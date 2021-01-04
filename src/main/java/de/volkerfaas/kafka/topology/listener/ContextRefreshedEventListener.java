package de.volkerfaas.kafka.topology.listener;

import de.volkerfaas.kafka.topology.bootstrap.PropertiesValidation;
import org.jetbrains.annotations.NotNull;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class ContextRefreshedEventListener implements ApplicationListener<ContextRefreshedEvent> {

    @Override
    public void onApplicationEvent(@NotNull final ContextRefreshedEvent event) {
        final ApplicationContext context = event.getApplicationContext();
        final Environment environment = context.getEnvironment();
        final boolean result = PropertiesValidation.init(environment).validateProperties().result();
        if (!result) {
            System.exit(SpringApplication.exit(context, () -> 1));
        }
    }

}
