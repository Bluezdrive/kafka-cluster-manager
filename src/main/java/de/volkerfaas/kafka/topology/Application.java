package de.volkerfaas.kafka.topology;

import de.volkerfaas.kafka.topology.bootstrap.Bootstrap;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.ConfigurableEnvironment;

import java.util.Locale;

@SpringBootApplication
public class Application implements InitializingBean {

    public static void main(final String[] args) {
        Locale.setDefault(Locale.ENGLISH);
        SpringApplication.run(Application.class, args);
    }

    private final ConfigurableEnvironment environment;

    @Autowired
    public Application(final ConfigurableEnvironment environment) {
        this.environment = environment;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Bootstrap.init(environment)
                .validateArguments()
                .handleArgumentHelp()
                .handleArgumentConfig()
                .loadClusterProperties()
                .verifyClusterPropertiesSet();
    }
}
