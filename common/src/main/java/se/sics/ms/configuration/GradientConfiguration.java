package se.sics.ms.configuration;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.Properties;

public final class GradientConfiguration {
	private final long period;
	private final long seed;
	private final double temperature;
	private final int viewSize;
	private final int exchangeCount;
	private final double convergenceSimilarity;

	/**
	 * @param seed
	 *            the seed used to create random numbers
	 * @param period
	 *            the period between the scheduled execution events
	 * @param temperature
	 *            the temperature used to select a good node with a high
	 *            probability
	 * @param viewSize
	 *            the size of the TMan view
	 * @param exchangeCount
	 *            the number of nodes exchanged between to TMan nodes
	 * @param convergenceSimilarity
	 *            the percentage of nodes allowed to change in the view in order
	 *            to be converged
	 */
	public GradientConfiguration(long seed, long period, double temperature, int viewSize,
			int exchangeCount, double convergenceSimilarity) {
		super();
		this.seed = seed;
		this.period = period;
		this.temperature = temperature;
		this.viewSize = viewSize;
		this.exchangeCount = exchangeCount;
		this.convergenceSimilarity = convergenceSimilarity;
	}

	/**
	 * @return the seed used to create random numbers
	 */
	public long getSeed() {
		return seed;
	}

	/**
	 * @return the period between the scheduled execution events
	 */
	public long getPeriod() {
		return this.period;
	}

	/**
	 * @return the temperature used to select a good node with a high
	 *         probability
	 */
	public double getTemperature() {
		return temperature;
	}

	/**
	 * @return the size of the TMan view
	 */
	public int getViewSize() {
		return viewSize;
                

	}

	/**
	 * @return the number of nodes exchanged between to TMan nodes.
	 */
	public int getExchangeCount() {
		return exchangeCount;
	}

	/**
	 * @return the percentage of nodes allowed to change in the view in order to
	 *         be converged
	 */
	public double getConvergenceSimilarity() {
		return convergenceSimilarity;
	}

	public void store(String file) throws IOException {
		Properties p = new Properties();
		p.setProperty("seed", "" + seed);
		p.setProperty("period", "" + period);
		p.setProperty("temperature", "" + temperature);
		p.setProperty("viewSize", "" + viewSize);
		p.setProperty("exchangeCount", "" + exchangeCount);
		p.setProperty("convergenceSimilarity", "" + convergenceSimilarity);

		Writer writer = new FileWriter(file);
		p.store(writer, "se.sics.kompics.p2p.overlay.application");
	}

	public static GradientConfiguration load(String file) throws IOException {
		Properties p = new Properties();
		Reader reader = new FileReader(file);
		p.load(reader);

		long seed = Long.parseLong(p.getProperty("seed"));
		long period = Long.parseLong(p.getProperty("period"));
		double temp = Double.parseDouble(p.getProperty("temperature"));
		int viewSize = Integer.parseInt(p.getProperty("viewSize"));
		int exchangeCount = Integer.parseInt(p.getProperty("exchangeCount"));
		double convergenceSimilarity = Double.parseDouble(p.getProperty("convergenceSimilarity"));

		return new GradientConfiguration(seed, period, temp, viewSize, exchangeCount,
				convergenceSimilarity);
	}
}
