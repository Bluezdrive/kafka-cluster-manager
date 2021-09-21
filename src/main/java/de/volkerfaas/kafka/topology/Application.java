package de.volkerfaas.kafka.topology;

import de.volkerfaas.kafka.topology.listener.ApplicationContextInitializedEventListener;
import de.volkerfaas.kafka.topology.listener.ContextRefreshedEventListener;
import de.volkerfaas.utils.CommandLineArguments;
import de.volkerfaas.utils.DefaultCommandLineArguments;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

import java.util.Locale;

import static de.volkerfaas.kafka.topology.KafkaClusterManagerCommandLineOption.*;

@SpringBootApplication
public class Application {

    public static void main(final String[] args) {
        Locale.setDefault(Locale.ENGLISH);
        final SpringApplication springApplication = new SpringApplicationBuilder(Application.class)
                .listeners(new ApplicationContextInitializedEventListener())
                .listeners(new ContextRefreshedEventListener())
                .banner(new KafkaClusterManagerBanner())
                .bannerMode(Banner.Mode.CONSOLE)
                .profiles(getProfile(args))
                .build();
        springApplication.run(args);
    }

    public static String getProfile(final String[] args) {
        final CommandLineArguments arguments = new DefaultCommandLineArguments(args);
        if (arguments.containsOption(DELETE) || arguments.containsOption(DEPLOY) || arguments.containsOption(RESTORE)) {
            return arguments.getPropertyValue(KafkaClusterManagerCommandLineProperty.CLUSTER);
        }
        return "default";
    }

}
