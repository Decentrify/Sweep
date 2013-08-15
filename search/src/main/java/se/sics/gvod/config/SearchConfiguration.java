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
    int maxExchangeCount;
    int queryTimeout;
    int addTimeout;
    int replicationTimeout;
    int retryCount;
    int hitsPerQuery;
    int recentRequestsGcInterval;
    int maxLeaderIdHistorySize;
    long maxEntriesOnPeer;

    public SearchConfiguration() {
        this(
                MsConfig.SEARCH_NUM_PARTITIONS,
                MsConfig.SEARCH_MAX_EXCHANGE_COUNT,
                MsConfig.SEARCH_QUERY_TIMEOUT,
                MsConfig.SEARCH_ADD_TIMEOUT,
                MsConfig.SEARCH_REPLICATION_TIMEOUT,
                MsConfig.SEARCH_RETRY_COUNT,
                MsConfig.SEARCH_HITS_PER_QUERY,
                MsConfig.SEARCH_RECENT_REQUESTS_GCINTERVAL,
                MsConfig.MAX_LEADER_ID_HISTORY_SIZE,
                MsConfig.MAX_ENTRIES_ON_PEER);
    }

    /**
     * @param numPartitions the number of partitions used to balance the load
     * @param maxExchangeCount the maximum number of addresses exchanged in one
     * exchange request
     * @param queryTimeout the amount of time until a search request times out
     * @param addTimeout the amount of time until an add request times out
     * @param replicationTimeout addTimeout the amount of time until an
     * replication request times out
     * @param retryCount the number retries executed if no acknowledgment for an
     * add operations was received
     * @param hitsPerQuery the maximum amount of entries reported for a search
     * request
     * @param recentRequestsGcInterval the interval used to garbage collect the
     * UUIDs of recente requests
     */
    public SearchConfiguration(
            int numPartitions, int maxExchangeCount, int queryTimeout,
            int addTimeout, int replicationTimeout,
            int retryCount, int hitsPerQuery, int recentRequestsGcInterval,
            int maxLeaderIdHistorySize, long maxEntriesOnPeer) {
        this.numPartitions = numPartitions;
        this.maxExchangeCount = maxExchangeCount;
        this.queryTimeout = queryTimeout;
        this.addTimeout = addTimeout;
        this.replicationTimeout = replicationTimeout;
        this.retryCount = retryCount;
        this.hitsPerQuery = hitsPerQuery;
        this.recentRequestsGcInterval = recentRequestsGcInterval;
        this.maxLeaderIdHistorySize = maxLeaderIdHistorySize;
        this.maxEntriesOnPeer = maxEntriesOnPeer;
    }

    public static SearchConfiguration build() {
        return new SearchConfiguration();
    }
    public int getNumPartitions() {
        return numPartitions;
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

    public int getRetryCount() {
        return retryCount;
    }

    public int getHitsPerQuery() {
        return hitsPerQuery;
    }

    public int getRecentRequestsGcInterval() {
        return recentRequestsGcInterval;
    }

    public int getMaxLeaderIdHistorySize() {
        return maxLeaderIdHistorySize;
    }

    public long getMaxEntriesOnPeer() {
        return maxEntriesOnPeer;
    }

    public SearchConfiguration setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
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

    public SearchConfiguration setRetryCount(int retryCount) {
        this.retryCount = retryCount;
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

    public void setMaxEntriesOnPeer(long maxEntriesOnPeer) {
        this.maxEntriesOnPeer = maxEntriesOnPeer;
    }
}
