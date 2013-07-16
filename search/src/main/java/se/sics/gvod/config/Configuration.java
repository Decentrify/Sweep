package se.sics.gvod.config;

import java.io.IOException;

/**
 * The configuration file for the hole program.
 */
public class Configuration extends CompositeConfiguration {
	public static int SNAPSHOT_PERIOD = 1000;
	public static int AVAILABLE_TOPICS = 20;

    CroupierConfiguration croupierConfiguration;
    GradientConfiguration gradientConfiguration;
    SearchConfiguration searchConfiguration;
    ElectionConfiguration electionConfiguration;

    public Configuration() throws IOException {
//		this.seed = seed;
//		searchConfiguration = new SearchConfiguration(10, 10, seed, 10, 25000, 10000, 20000, 3, 3,
//				5, 5000, 4, 5000, 100, 60000);
//		tmanConfiguration = new GradientConfiguration(seed, 1000, 0.8, 10, 10, 0.8);
        croupierConfiguration = CroupierConfiguration.build()
                .setRto(3000)
                .setRtoRetries(2)
                .setRtoScale(1.0d);
        gradientConfiguration = GradientConfiguration.build();
        electionConfiguration = ElectionConfiguration.build();
        searchConfiguration = SearchConfiguration.build();

//		electionConfiguration = new ElectionConfiguration(8, 4, 0.7, 5000, 1000, 0, 0.7, 2000,
//				2000, 4000, 0.8, 8, 10000, true);
    }

    public CroupierConfiguration getCroupierConfiguration() {
        return croupierConfiguration;
    }

    public ElectionConfiguration getElectionConfiguration() {
        return electionConfiguration;
    }

    public GradientConfiguration getGradientConfiguration() {
        return gradientConfiguration;
    }

    public SearchConfiguration getSearchConfiguration() {
        return searchConfiguration;
    }
}
