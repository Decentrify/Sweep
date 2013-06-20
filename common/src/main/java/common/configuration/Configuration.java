package common.configuration;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import se.sics.kompics.address.Address;
import se.sics.kompics.p2p.bootstrap.BootstrapConfiguration;

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
	int bootId = Integer.MAX_VALUE;
	int networkPort = 8081;
	Address bootServerAddress = new Address(ip, networkPort, bootId);
	final long seed;
	BootstrapConfiguration bootConfiguration = new BootstrapConfiguration(bootServerAddress,
			10 * 60 * 1000, 4000, 3, 30000, webPort, webPort);
	CyclonConfiguration cyclonConfiguration;
	TManConfiguration tmanConfiguration;
	SearchConfiguration searchConfiguration;
	ElectionConfiguration electionConfiguration;

	public Configuration(long seed) throws IOException {
		this.seed = seed;
		searchConfiguration = new SearchConfiguration(10, 10, seed, 10, 25000, 10000, 20000, 3, 3,
				5, 5000, 4, 5000, 100, 60000);
		tmanConfiguration = new TManConfiguration(seed, 1000, 0.8, 10, 10, 0.8);
		cyclonConfiguration = new CyclonConfiguration(seed, 5, 10, 1000, 500000,
				(long) (Integer.MAX_VALUE - Integer.MIN_VALUE), 20);
		electionConfiguration = new ElectionConfiguration(8, 4, 0.7, 5000, 1000, 0, 0.7, 2000,
				2000, 4000, 0.8, 8, 10000, true);

		String c = File.createTempFile("bootstrap.", ".conf").getAbsolutePath();
		bootConfiguration.store(c);
		System.setProperty("bootstrap.configuration", c);

		c = File.createTempFile("cyclon.", ".conf").getAbsolutePath();
		cyclonConfiguration.store(c);
		System.setProperty("cyclon.configuration", c);

		c = File.createTempFile("tman.", ".conf").getAbsolutePath();
		tmanConfiguration.store(c);
		System.setProperty("tman.configuration", c);

		c = File.createTempFile("search.", ".conf").getAbsolutePath();
		searchConfiguration.store(c);
		System.setProperty("search.configuration", c);

		c = File.createTempFile("election.", ".conf").getAbsolutePath();
		electionConfiguration.store(c);
		System.setProperty("election.configuration", c);
	}
}
