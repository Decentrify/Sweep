/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.gvod.config;

import se.sics.ms.configuration.MsConfig;


/**
 *
 * @author jdowling
 */
public class SearchConfiguration
        extends AbstractConfiguration<se.sics.gvod.config.SearchConfiguration> {

    int numPartitions;
    int maxNumRoutingEntries;
    int maxExchangeCount;
    int queryTimeout;
    int addTimeout;
    int replicationTimeout;
    int replicationMaximum;
    int replicationMinimum;
    int retryCount;
    int gapTimeout;
    int hitsPerQuery;
    int recentRequestsGcInterval;

    public SearchConfiguration() {
        this(
                MsConfig.SEARCH_NUM_PARTITIONS,
                MsConfig.SEARCH_MAX_NUM_ROUTING_ENTRIES,
                MsConfig.SEARCH_MAX_EXCHANGE_COUNT,
                MsConfig.SEARCH_QUERY_TIMEOUT,
                MsConfig.SEARCH_ADD_TIMEOUT,
                MsConfig.SEARCH_REPLICATION_TIMEOUT,
                MsConfig.SEARCH_REPLICATION_MAXIMUM,
                MsConfig.SEARCH_REPLICATION_MINIMUM,
                MsConfig.SEARCH_RETRY_COUNT,
                MsConfig.SEARCH_GAP_TIMEOUT,
                MsConfig.SEARCH_HITS_PER_QUERY,
                MsConfig.SEARCH_RECENT_REQUESTS_GCINTERVAL);
    }

    /**
     * @param numPartitions the number of partitions used to balance the load
     * @param maxNumRoutingEntries the maximum number of entries for each bucket
     * in the routing table
     * @param maxExchangeCount the maximum number of addresses exchanged in one
     * exchange request
     * @param queryTimeout the amount of time until a search request times out
     * @param addTimeout the amount of time until an add request times out
     * @param replicationTimeout addTimeout the amount of time until an
     * replication request times out
     * @param replicationMaximum the maximum number of replication requests sent
     * @param replicationMinimum the minimum number of required replication
     * acknowledgments
     * @param retryCount the number retries executed if no acknowledgment for an
     * add operations was received
     * @param gapTimeout the amount of time until a gap detection is issued
     * @param hitsPerQuery the maximum amount of entries reported for a search
     * request
     * @param recentRequestsGcInterval the interval used to garbage collect the
     * UUIDs of recente requests
     */
    public SearchConfiguration(
            int numPartitions, int maxNumRoutingEntries, int maxExchangeCount, int queryTimeout,
            int addTimeout, int replicationTimeout, int replicationMaximum, int replicationMinimum,
            int retryCount, int gapTimeout, int hitsPerQuery, int recentRequestsGcInterval) {
        this.numPartitions = numPartitions;
        this.maxNumRoutingEntries = maxNumRoutingEntries;
        this.maxExchangeCount = maxExchangeCount;
        this.queryTimeout = queryTimeout;
        this.addTimeout = addTimeout;
        this.replicationTimeout = replicationTimeout;
        this.replicationMaximum = replicationMaximum;
        this.replicationMinimum = replicationMinimum;
        this.retryCount = retryCount;
        this.gapTimeout = gapTimeout;
        this.hitsPerQuery = hitsPerQuery;
        this.recentRequestsGcInterval = recentRequestsGcInterval;
    }

    public static SearchConfiguration build() {
        return new SearchConfiguration();
    }
    public int getNumPartitions() {
        return numPartitions;
    }

    public int getMaxNumRoutingEntries() {
        return maxNumRoutingEntries;
    }

    public int getMaxExchangeCount() {
        return maxExchangeCount;
    }

    public int getQueryTimeout() {
        return queryTimeout;
    }

    public int getAddTimeout() {
        return addTimeout;
    }

    public int getReplicationTimeout() {
        return replicationTimeout;
    }

    public int getReplicationMaximum() {
        return replicationMaximum;
    }

    public int getReplicationMinimum() {
        return replicationMinimum;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public int getGapTimeout() {
        return gapTimeout;
    }

    public int getHitsPerQuery() {
        return hitsPerQuery;
    }

    public int getRecentRequestsGcInterval() {
        return recentRequestsGcInterval;
    }

    public SearchConfiguration setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
        return this;
    }

    public SearchConfiguration setMaxNumRoutingEntries(int maxNumRoutingEntries) {
        this.maxNumRoutingEntries = maxNumRoutingEntries;
        return this;
    }

    public SearchConfiguration setMaxExchangeCount(int maxExchangeCount) {
        this.maxExchangeCount = maxExchangeCount;
        return this;
    }

    public SearchConfiguration setQueryTimeout(int queryTimeout) {
        this.queryTimeout = queryTimeout;
        return this;
    }

    public SearchConfiguration setAddTimeout(int addTimeout) {
        this.addTimeout = addTimeout;
        return this;
    }

    public SearchConfiguration setReplicationTimeout(int replicationTimeout) {
        this.replicationTimeout = replicationTimeout;
        return this;
    }

    public SearchConfiguration setReplicationMaximum(int replicationMaximum) {
        this.replicationMaximum = replicationMaximum;
        return this;
    }

    public SearchConfiguration setReplicationMinimum(int replicationMinimum) {
        this.replicationMinimum = replicationMinimum;
        return this;
    }

    public SearchConfiguration setRetryCount(int retryCount) {
        this.retryCount = retryCount;
        return this;
    }

    public SearchConfiguration setGapTimeout(int gapTimeout) {
        this.gapTimeout = gapTimeout;
        return this;
    }

    public SearchConfiguration setHitsPerQuery(int hitsPerQuery) {
        this.hitsPerQuery = hitsPerQuery;
        return this;
    }

    public SearchConfiguration setRecentRequestsGcInterval(int recentRequestsGcInterval) {
        this.recentRequestsGcInterval = recentRequestsGcInterval;
        return this;
    }
}
