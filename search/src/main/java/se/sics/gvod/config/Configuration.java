package se.sics.gvod.config;

import java.io.IOException;

/**
 * The configuration file for the whole program.
 * You invoke the store method to store all the configuration
 * files that will be later used by the simulator.
 */
public class Configuration extends CompositeConfiguration {

    public static int SNAPSHOT_PERIOD = 30 * 1000;
    public static int AVAILABLE_TOPICS = 20;
    CroupierConfiguration croupierConfiguration;
    GradientConfiguration gradientConfiguration;
    SearchConfiguration searchConfiguration;

    public Configuration() throws IOException {
        croupierConfiguration = CroupierConfiguration.build()
                .setRto(3000)
                .setRtoRetries(2)
                .setRtoScale(1.0d);
        gradientConfiguration = GradientConfiguration.build();
        searchConfiguration = SearchConfiguration.build();
    }
}
