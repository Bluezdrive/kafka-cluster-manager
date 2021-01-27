package de.volkerfaas.kafka.topology;

import de.volkerfaas.kafka.topology.listener.ApplicationContextInitializedEventListener;
import de.volkerfaas.kafka.topology.listener.ContextRefreshedEventListener;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

import java.util.Locale;

@SpringBootApplication
public class Application {

    public static void main(final String[] args) {
        Locale.setDefault(Locale.ENGLISH);
        final SpringApplication springApplication = new SpringApplicationBuilder(Application.class)
                .listeners(new ApplicationContextInitializedEventListener())
                .listeners(new ContextRefreshedEventListener())
                .banner(new KafkaClusterManagerBanner())
                .bannerMode(Banner.Mode.CONSOLE)
                .build();
        springApplication.run(args);
    }

}
