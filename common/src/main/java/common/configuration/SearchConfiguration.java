package common.configuration;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.Properties;

public final class SearchConfiguration {
	private final int numPartitions;
	private final int maxNumRoutingEntries;
	private final long seed;
	private final int maxExchangeCount;
	private final int searchTimeout;
	private final int addTimeout;
	private final int replicationTimeout;
	private final int replicationMaximum;
	private final int replicationMinimum;
	private final int retryCount;
	private final int gapTimeout;
	private final int gapDetectionTtl;
	private final int gapDetectionTimeout;
	private final int hitsPerQuery;
	private final int recentRequestsGcInterval;

	/**
	 * @param numPartitions
	 *            the number of partitions used to balance the load
	 * @param maxNumRoutingEntries
	 *            the maximum number of entries for each bucket in the routing
	 *            table
	 * @param seed
	 *            the seed to create random numbers
	 * @param maxExchangeCount
	 *            the maximum number of addresses exchanged in one exchange
	 *            request
	 * @param searchTimeout
	 *            the amount of time until a search request times out
	 * @param addTimeout
	 *            the amount of time until an add request times out
	 * @param replicationTimeout
	 *            addTimeout the amount of time until an replication request
	 *            times out
	 * @param replicationMaximum
	 *            the maximum number of replication requests sent
	 * @param replicationMinimum
	 *            the minimum number of required replication acknowledgments
	 * @param retryCount
	 *            the number retries executed if no acknowledgment for an add
	 *            operations was received
	 * @param gapTimeout
	 *            the amount of time until a gap detection is issued
	 * @param gapDetectionTtl
	 *            the TTL for the random walks used for gap detection
	 * @param gapDetectionTimeout
	 *            the amount of time until the collected answers for a gap
	 *            detection random walk are evaluated
	 * @param hitsPerQuery
	 *            the maximum amount of entries reported for a search request
	 * @param recentRequestsGcInterval
	 *            the interval used to garbage collect the UUIDs of recente
	 *            requests
	 */
	public SearchConfiguration(int numPartitions, int maxNumRoutingEntries, long seed,
			int maxExchangeCount, int searchTimeout, int addTimeout, int replicationTimeout,
			int replicationMaximum, int replicationMinimum, int retryCount, int gapTimeout,
			int gapDetectionTtl, int gapDetectionTimeout, int hitsPerQuery,
			int recentRequestsGcInterval) {
		this.numPartitions = numPartitions;
		this.maxNumRoutingEntries = maxNumRoutingEntries;
		this.seed = seed;
		this.maxExchangeCount = maxExchangeCount;
		this.searchTimeout = searchTimeout;
		this.addTimeout = addTimeout;
		this.replicationTimeout = replicationTimeout;
		this.replicationMaximum = replicationMaximum;
		this.replicationMinimum = replicationMinimum;
		this.retryCount = retryCount;
		this.gapTimeout = gapTimeout;
		this.gapDetectionTtl = gapDetectionTtl;
		this.gapDetectionTimeout = gapDetectionTimeout;
		this.hitsPerQuery = hitsPerQuery;
		this.recentRequestsGcInterval = recentRequestsGcInterval;
	}

	/**
	 * @return the number of partitions used to balance the load
	 */
	public int getNumPartitions() {
		return numPartitions;
	}

	/**
	 * @return the maximum number of entries for each bucket in the routing
	 *         table
	 */
	public int getMaxNumRoutingEntries() {
		return maxNumRoutingEntries;
	}

	/**
	 * @return the seed to create random numbers
	 */
	public long getSeed() {
		return seed;
	}

	/**
	 * @return the maximum number of addresses exchanged in one exchange request
	 */
	public int getMaxExchangeCount() {
		return maxExchangeCount;
	}

	/**
	 * @return the amount of time until a search request times out
	 */
	public int getSearchTimeout() {
		return searchTimeout;
	}

	/**
	 * @return the amount of time until an add request times out
	 */
	public int getAddTimeout() {
		return addTimeout;
	}

	/**
	 * @return addTimeout the amount of time until an replication request times
	 *         out
	 */
	public int getReplicationTimeout() {
		return replicationTimeout;
	}

	/**
	 * @return the maximum number of replication requests sent
	 */
	public int getReplicationMaximum() {
		return replicationMaximum;
	}

	/**
	 * @return the minimum number of required replication acknowledgments
	 */
	public int getReplicationMinimum() {
		return replicationMinimum;
	}

	/**
	 * @return the number retries executed if no acknowledgment for an add
	 *         operations was received
	 */
	public int getRetryCount() {
		return retryCount;
	}

	/**
	 * @return the amount of time until a gap detection is issued
	 */
	public int getGapTimeout() {
		return gapTimeout;
	}

	/**
	 * @return the TTL for the random walks used for gap detection
	 */
	public int getGapDetectionTtl() {
		return gapDetectionTtl;
	}

	/**
	 * @return the amount of time until the collected answers for a gap
	 *         detection random walk are evaluated
	 */
	public int getGapDetectionTimeout() {
		return gapDetectionTimeout;
	}

	/**
	 * @return the maximum amount of entries reported for a search request
	 */
	public int getHitsPerQuery() {
		return hitsPerQuery;
	}

	/**
	 * @return the interval used to garbage collect the UUIDs of recente
	 *         requests
	 */
	public int getRecentRequestsGcInterval() {
		return recentRequestsGcInterval;
	}

	public void store(String file) throws IOException {
		Properties p = new Properties();
		p.setProperty("numPartitions", "" + numPartitions);
		p.setProperty("maxNumRoutingEntries", "" + maxNumRoutingEntries);
		p.setProperty("seed", "" + seed);
		p.setProperty("maxExchangeCount", "" + maxExchangeCount);
		p.setProperty("searchTimeout", "" + searchTimeout);
		p.setProperty("addTimeout", "" + addTimeout);
		p.setProperty("replicationTimeout", "" + replicationTimeout);
		p.setProperty("replicationMaximum", "" + replicationMaximum);
		p.setProperty("replicationMinimum", "" + replicationMinimum);
		p.setProperty("retryCount", "" + retryCount);
		p.setProperty("gapTimeout", "" + gapTimeout);
		p.setProperty("gapDetectionTtl", "" + gapDetectionTtl);
		p.setProperty("gapDetectionTimeout", "" + gapDetectionTimeout);
		p.setProperty("hitsPerQuery", "" + hitsPerQuery);
		p.setProperty("recentRequestsGcInterval", "" + recentRequestsGcInterval);

		Writer writer = new FileWriter(file);
		p.store(writer, "se.sics.kompics.p2p.overlay.application");
	}

	public static SearchConfiguration load(String file) throws IOException {
		Properties p = new Properties();
		Reader reader = new FileReader(file);
		p.load(reader);

		int numPartitions = Integer.parseInt(p.getProperty("numPartitions"));
		int maxNumRoutingEntries = Integer.parseInt(p.getProperty("maxNumRoutingEntries"));
		long seed = Long.parseLong(p.getProperty("seed"));
		int maxExchangeCount = Integer.parseInt(p.getProperty("maxExchangeCount"));
		int searchTimeout = Integer.parseInt(p.getProperty("searchTimeout"));
		int addTimeout = Integer.parseInt(p.getProperty("addTimeout"));
		int replicationTimeout = Integer.parseInt(p.getProperty("replicationTimeout"));
		int replicationMaximum = Integer.parseInt(p.getProperty("replicationMaximum"));
		int replicationMinimum = Integer.parseInt(p.getProperty("replicationMinimum"));
		int retryCount = Integer.parseInt(p.getProperty("retryCount"));
		int gapTimeout = Integer.parseInt(p.getProperty("gapTimeout"));
		int gapDetectionTtl = Integer.parseInt(p.getProperty("gapDetectionTtl"));
		int gapDetectionTimeout = Integer.parseInt(p.getProperty("gapDetectionTimeout"));
		int hitsPerQuery = Integer.parseInt(p.getProperty("hitsPerQuery"));
		int recentRequestsGcInterval = Integer.parseInt(p.getProperty("recentRequestsGcInterval"));

		return new SearchConfiguration(numPartitions, maxNumRoutingEntries, seed, maxExchangeCount,
				searchTimeout, addTimeout, replicationTimeout, replicationMaximum,
				replicationMinimum, retryCount, gapTimeout, gapDetectionTtl, gapDetectionTimeout,
				hitsPerQuery, recentRequestsGcInterval);
	}
}
