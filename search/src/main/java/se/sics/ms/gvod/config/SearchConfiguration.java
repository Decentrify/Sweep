/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.ms.gvod.config;

import se.sics.ms.configuration.MsConfig;


/**
 *
 * @author jdowling
 */
public class SearchConfiguration {

    int maximumEpochUpdatesPullSize;
    int maxEpochContainerSize;
    int maxExchangeCount;
    int queryTimeout;
    int addTimeout;
    int replicationTimeout;
    int retryCount;
    int hitsPerQuery;
    int recentRequestsGcInterval;
    int maxLeaderIdHistorySize;
    long maxEntriesOnPeer;
    int maxSearchResults;
    int indexExchangeTimeout;
    int indexExchangeRequestNumber;
    int maxPartitionIdLength;
    int controlExchangeTimePeriod;
    int delayedPartitioningRequestTimeout;
    int controlMessageEnumSize;
    int leaderGroupSize;
    int partitionPrepareTimeout;
    int partitionCommitRequestTimeout;
    int partitionCommitTimeout;
    int indexExchangePeriod;


    //TODO: Add the entries regarding the control messages in the system.

    public SearchConfiguration() {
        this(
                MsConfig.SEARCH_MAX_EXCHANGE_COUNT,
                MsConfig.SEARCH_QUERY_TIMEOUT,
                MsConfig.SEARCH_ADD_TIMEOUT,
                MsConfig.SEARCH_REPLICATION_TIMEOUT,
                MsConfig.SEARCH_RETRY_COUNT,
                MsConfig.SEARCH_HITS_PER_QUERY,
                MsConfig.SEARCH_RECENT_REQUESTS_GCINTERVAL,
                MsConfig.MAX_LEADER_ID_HISTORY_SIZE,
                MsConfig.SEARCH_MAX_SEARCH_RESULTS,
                MsConfig.SEARCH_INDEX_EXCHANGE_TIMEOUT,
                MsConfig.SEARCH_INDEX_EXCHANGE_REQUEST_NUMBER,
                MsConfig.MAX_ENTRIES_ON_PEER,
                MsConfig.MAX_PARTITION_ID_LENGTH,
                MsConfig.CONTROL_MESSAGE_EXCHANGE_PERIOD,
                MsConfig.DELAYED_PARTITIONING_REQUEST_TIMEOUT,
                MsConfig.LEADER_GROUP_SIZE,
                MsConfig.PARTITION_PREPARE_TIMEOUT,
                MsConfig.PARTITION_COMMIT_REQUEST_TIMEOUT,
                MsConfig.PARTITION_COMMIT_TIMEOUT,
                MsConfig.CONTROL_MESSAGE_ENUM_SIZE,
                MsConfig.INDEX_EXCHANGE_PERIOD,
                MsConfig.MAX_EPOCH_UPDATES,
                MsConfig.MAX_EPOCH_CONTAINER_ENTRIES);
    }

    /**
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
     * @param recentRequestsGcInterval the interval used to garbage collect the UUIDs of recent requests
     * @param maxSearchResults max number of results per search
     * @param indexExchangeTimeout max time to wait for index exchange responses
     * @param indexExchangeRequestNumber the number of nodes to query for hashes and compare them during the index request process
     *
     */
    public SearchConfiguration(
            int maxExchangeCount, int queryTimeout,
            int addTimeout, int replicationTimeout,
            int retryCount, int hitsPerQuery, int recentRequestsGcInterval,
            int maxLeaderIdHistorySize, int maxSearchResults,
            int indexExchangeTimeout, int indexExchangeRequestNumber,
            long maxEntriesOnPeer, int maxPartitionIdLength,
            int controlExchangeTimePeriod, int delayedPartitioningRequestTimeout,
            int leaderGroupSize, int partitionPrepareTimeout,
            int partitionCommitRequestTimeout, int partitionCommitTimeout,
            int controlMessageEnumSize, int indexExchangePeriod,
            int maximumEpochUpdatesPullSize, int maxEpochContainerSize) {
        this.maxExchangeCount = maxExchangeCount;
        this.queryTimeout = queryTimeout;
        this.addTimeout = addTimeout;
        this.replicationTimeout = replicationTimeout;
        this.retryCount = retryCount;
        this.hitsPerQuery = hitsPerQuery;
        this.recentRequestsGcInterval = recentRequestsGcInterval;
        this.maxLeaderIdHistorySize = maxLeaderIdHistorySize;
        this.maxEntriesOnPeer = maxEntriesOnPeer;
        this.maxSearchResults = maxSearchResults;
        this.indexExchangeTimeout = indexExchangeTimeout;
        this.indexExchangeRequestNumber = indexExchangeRequestNumber;
        this.maxPartitionIdLength = maxPartitionIdLength;
        this.controlExchangeTimePeriod = controlExchangeTimePeriod;
        this.delayedPartitioningRequestTimeout = delayedPartitioningRequestTimeout;
        this.leaderGroupSize = leaderGroupSize;
        this.partitionPrepareTimeout = partitionPrepareTimeout;
        this.partitionCommitRequestTimeout = partitionCommitRequestTimeout;
        this.partitionCommitTimeout = partitionCommitTimeout;
        this.controlMessageEnumSize = controlMessageEnumSize;
        this.indexExchangePeriod = indexExchangePeriod;
        this.maximumEpochUpdatesPullSize = maximumEpochUpdatesPullSize;
        this.maxEpochContainerSize = maxEpochContainerSize;
    }

    public static SearchConfiguration build() {
        return new SearchConfiguration();
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

    public int getLeaderGroupSize() {
        return leaderGroupSize;
    }

    public int getPartitionPrepareTimeout() {
        return partitionPrepareTimeout;
    }

    public int getPartitionCommitRequestTimeout() {
        return partitionCommitRequestTimeout;
    }

    public int getPartitionCommitTimeout() {
        return partitionCommitTimeout;
    }

    public int getDelayedPartitioningRequestTimeout() {
        return delayedPartitioningRequestTimeout;
    }

    public int getControlMessageEnumSize(){
        return this.controlMessageEnumSize;
    }

    public int getControlExchangeTimePeriod(){
        return this.controlExchangeTimePeriod;
    }

    public long getMaxEntriesOnPeer() {
        return maxEntriesOnPeer;
    }

    public int getMaxSearchResults() {
        return maxSearchResults;
    }

    public int getIndexExchangePeriod() { return indexExchangePeriod; }

    public int getMaximumEpochUpdatesPullSize() {
        return maximumEpochUpdatesPullSize;
    }


    public int getMaxEpochContainerSize() {
        return maxEpochContainerSize;
    }

    public void setMaximumEpochUpdatesPullSize(int maximumEpochUpdatesPullSize) {
        this.maximumEpochUpdatesPullSize = maximumEpochUpdatesPullSize;
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

    public SearchConfiguration setMaxSearchResults(int maxSearchResults) {
        this.maxSearchResults = maxSearchResults;
        return this;
    }

    public int getIndexExchangeTimeout() {
        return indexExchangeTimeout;
    }

    public int getIndexExchangeRequestNumber() {
        return indexExchangeRequestNumber;
    }

    public int getMaxPartitionIdLength() {
        return maxPartitionIdLength;
    }
}
