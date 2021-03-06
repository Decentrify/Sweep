/**
 * This file is part of the Kompics P2P Framework.
 *
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS)
 * Copyright (C) 2009 Royal Institute of Technology (KTH)
 *
 * Kompics is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR QueryLimit PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.ms.gvod.config;

import se.sics.ms.configuration.MsConfig;

/**
 *
 * @author jim
 */
public class GradientConfiguration 
{

    /** 
     * Fields cannot be private. Package protected, ok.
     */
    // Max Number of references to neighbours stored in a gradient view
    int viewSize;
    // Number of best similar peers that should be sent in each sets exchange response.
    int shuffleLength; //10
    // How often to exchange gradient views with a neighbour
    int shufflePeriod;
    // When searching up the Gradient, ???
    int utilityThreshold;
    // Fingers are long-range links (small world links) to nodes higher up the gradient
    int numFingers; 
    // Range is from 0-1.0. Higher temperature (nearer 1) causes the gradient to converge slower, 
    // but with lower probability of having multiple nodes thinking they are the leader.
    // Lower temperatures (nearer 0.5) cause increasingly random neighbour selection.
    double temperature; 
    
    // Timeout for search msgs up the Gradient
    int searchRequestTimeout;
    // Number of parallel probes to send
    int numParallelSearches; // 5
    //Time-To-Live used for Gradient search messages.
    int searchTtl; //5
    // convergence similarity test
    double convergenceTest;
    // How many rounds does the convergenceTest need to be valid until the view is stated converged
    int convergenceTestRounds;
    int rto;
    int maxNumRoutingEntries;
    int leaderLookupTimeout;
    int searchParallelism;
    int latestRttStoreLimit;
    double rttAnomalyTolerance;
    int maxPartitionHistorySize;

    /**
     * Default constructor comes first.
     */
    public GradientConfiguration() {
        this(
                MsConfig.GRADIENT_VIEW_SIZE,
                MsConfig.GRADIENT_SHUFFLE_LENGTH,
                MsConfig.GRADIENT_SHUFFLE_PERIOD,
                MsConfig.GRADIENT_UTILITY_THRESHOLD,
                MsConfig.GRADIENT_NUM_FINGERS,
                MsConfig.GRADIENT_TEMPERATURE,
                MsConfig.GRADIENT_SEARCH_TIMEOUT,
                MsConfig.GRADIENT_NUM_PARALLEL_SEARCHES,
                MsConfig.GRADIENT_SEARCH_TTL,
                MsConfig.GRADIENT_CONVERGENCE_TEST,
                MsConfig.GRADIENT_CONVERGENCE_TEST_ROUNDS,
                MsConfig.GRADIENT_SHUFFLE_TIMEOUT,
                MsConfig.GRADIENT_MAX_NUM_ROUTING_ENTRIES,
                MsConfig.GRADIENT_LEADER_LOOKUP_TIMEOUT,
                MsConfig.GRADIENT_SEARCH_PARALLELISM,
                MsConfig.GRADIENT_LATEST_RTT_STORE_LIMIT,
                MsConfig.GRADIENT_RTT_ANOMALY_TOLERANCE,
                MsConfig.MAX_PARTITION_HISTORY_SIZE
                );
    }

    /** 
     * Full argument constructor comes second.
     */
    public GradientConfiguration(
            int viewSize, 
            int shuffleLength,
            int shufflePeriod,
            int utilityThreshold,
            int numFingers,
            double temperature,
            int searchRequestTimeout,
            int numParallelSearches,
            int searchTtl,
            double convergenceTest,
            int convergenceTestRounds,
            int rto,
            int maxNumRoutingEntries,
            int leaderLookupTimeout,
            int searchParallelism,
            int latestRttStoreLimit,
            double rttAnomalyTolerance,
            int maxPartitionHistorySize
            ) {
        this.viewSize = viewSize;
        this.shuffleLength = shuffleLength;
        this.shufflePeriod = shufflePeriod;
        this.searchRequestTimeout = searchRequestTimeout;
        this.temperature = temperature;
        this.utilityThreshold = utilityThreshold;
        this.numParallelSearches = numParallelSearches;
        this.searchTtl = searchTtl;
        this.numFingers = numFingers;
        this.convergenceTest = convergenceTest;
        this.convergenceTestRounds = convergenceTestRounds;
        this.rto = rto;
        this.maxNumRoutingEntries = maxNumRoutingEntries;
        this.leaderLookupTimeout = leaderLookupTimeout;
        this.searchParallelism = searchParallelism;
        this.latestRttStoreLimit = latestRttStoreLimit;
        this.rttAnomalyTolerance = rttAnomalyTolerance;
        this.maxPartitionHistorySize = maxPartitionHistorySize;
    }

    public static GradientConfiguration build() {
        return new GradientConfiguration();
    }

    public int getConvergenceTestRounds() {
        return convergenceTestRounds;
    }

    public double getConvergenceTest() {
        return convergenceTest;
    }
    
    public double getTemperature() {
        return temperature;
    }

    public int getShuffleLength() {
        return shuffleLength;
    }

    public int getShufflePeriod() {
        return shufflePeriod;
    }
    
    
    public int getViewSize() {
        return viewSize;
    }

    public int getSearchTtl() {
        return searchTtl;
    }

    public int getRto() {
        return rto;
    }

    public int getFingers() {
        return numFingers;
    }

    public int getNumParallelSearches() {
        return numParallelSearches;
    }

    public int getLeaderLookupTimeout() {
        return leaderLookupTimeout;
    }

    /**
     * @return the probeRequestTimeout
     */
    public int getSearchRequestTimeout() {
        return searchRequestTimeout;
    }

    public int getSearchParallelism() {
        return searchParallelism;
    }

    /**
     * @return the utilityThreshold
     */
    public int getUtilityThreshold() {
        return utilityThreshold;
    }

    public int getMaxNumRoutingEntries() {
        return maxNumRoutingEntries;
    }

    public int getLatestRttStoreLimit() {
        return latestRttStoreLimit;
    }

    public double getRttAnomalyTolerance() {
        return rttAnomalyTolerance;
    }

    public GradientConfiguration setViewSize(int viewSize) {
        this.viewSize = viewSize;
        return this;
    }

    public GradientConfiguration setShufflePeriod(int shufflePeriod) {
        this.shufflePeriod = shufflePeriod;
        return this;
    }

    public GradientConfiguration setRto(int rto) {
        this.rto = rto;
        return this;
    }

    public GradientConfiguration setSearchRequestTimeout(int searchRequestTimeout) {
        this.searchRequestTimeout = searchRequestTimeout;
        return this;
    }

    public GradientConfiguration setUtilityThreshold(int utilityThreshold) {
        this.utilityThreshold = utilityThreshold;
        return this;
    }

    public GradientConfiguration setNumParallelSearches(int numParallelSearches) {
        this.numParallelSearches = numParallelSearches;
        return this;
    }

    public GradientConfiguration setSearchTtl(int searchTtl) {
        this.searchTtl = searchTtl;
        return this;
    }

    public GradientConfiguration setShuffleLength(int shuffleLength) {
        this.shuffleLength = shuffleLength;
        return this;
    }

    public GradientConfiguration setNumFingers(int numFingers) {
        this.numFingers = numFingers;
        return this;
    }

    public GradientConfiguration setTemperature(double temperature) {
        this.temperature = temperature;
        return this;
    }
    
    public GradientConfiguration setConvergenceTest(double convergenceTest) {
        this.convergenceTest = convergenceTest;
        return this;
    }

    public GradientConfiguration setConvergenceTestRounds(int convergenceTestRounds) {
        this.convergenceTestRounds = convergenceTestRounds;
        return this;
    }

    public GradientConfiguration setMaxNumRoutingEntries(int maxNumRoutingEntries) {
        this.maxNumRoutingEntries = maxNumRoutingEntries;
        return this;
    }

    public GradientConfiguration setLeaderLookupTimeout(int leaderLookupTimeout) {
        this.leaderLookupTimeout = leaderLookupTimeout;
        return this;
    }

    public GradientConfiguration setSearchParallelism(int searchParallelism) {
        this.searchParallelism = searchParallelism;
        return this;
    }

    public GradientConfiguration setLatestRttStoreLimit(int latestRttStoreLimit) {
        this.latestRttStoreLimit = latestRttStoreLimit;
        return this;
    }

    public GradientConfiguration setRttAnomalyTolerance(double rttAnomalyTolerance) {
        this.rttAnomalyTolerance = rttAnomalyTolerance;
        return this;
    }

    public void setMaxPartitionHistorySize(int maxPartitionHistorySize) {
        this.maxPartitionHistorySize = maxPartitionHistorySize;
    }

    public int getMaxPartitionHistorySize() {
        return maxPartitionHistorySize;
    }
}
