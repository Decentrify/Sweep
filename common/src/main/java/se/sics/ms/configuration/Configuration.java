package se.sics.ms.configuration;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import se.sics.gvod.config.CroupierConfiguration;

/**
 * The configuration file for the hole program.
 */
public class Configuration {
	public static int SNAPSHOT_PERIOD = 1000;
	public static int AVAILABLE_TOPICS = 20;
	public InetAddress ip = null;
	{
		try {
			ip = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
		}
	}

	int webPort = 8080;
	final long seed;
	   CroupierConfiguration croupierConfiguration;
	GradientConfiguration tmanConfiguration;
	SearchConfiguration searchConfiguration;
	ElectionConfiguration electionConfiguration;

	public Configuration(long seed) throws IOException {
		this.seed = seed;
		searchConfiguration = new SearchConfiguration(10, 10, seed, 10, 25000, 10000, 20000, 3, 3,
				5, 5000, 4, 5000, 100, 60000);
		tmanConfiguration = new GradientConfiguration(seed, 1000, 0.8, 10, 10, 0.8);
		croupierConfiguration = CroupierConfiguration.build()
                        .setRto(3000)
                        .setRtoRetries(2)
                        .setRtoScale(1.0d);
                
                
                
		electionConfiguration = new ElectionConfiguration(8, 4, 0.7, 5000, 1000, 0, 0.7, 2000,
				2000, 4000, 0.8, 8, 10000, true);


		croupierConfiguration.store((int) seed);

//		c = File.createTempFile("tman.", ".conf").getAbsolutePath();
//		tmanConfiguration.store(c);
//		System.setProperty("tman.configuration", c);
//
//		c = File.createTempFile("search.", ".conf").getAbsolutePath();
//		searchConfiguration.store(c);
//		System.setProperty("search.configuration", c);
//
//		c = File.createTempFile("election.", ".conf").getAbsolutePath();
//		electionConfiguration.store(c);
//		System.setProperty("election.configuration", c);
	}
}
