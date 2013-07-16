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
public class ElectionConfiguration
        extends AbstractConfiguration<se.sics.gvod.config.ElectionConfiguration>
{
	int indexTimeout;
	int deathTimeout;
	int rejectedTimeout;
	boolean nodeSuggestion;
	int voteRequestTimeout;
	int heartbeatWaitTimeout;
	int heartbeatTimeoutDelay;
	int minSizeOfElectionGroup;
	double minPercentageOfVotes;
	int waitForNoOfIndexMessages;
	int heartbeatTimeoutInterval;
	int minNumberOfConvergedNodes;
	double deathVoteMajorityPercentage;
	double leaderDeathMajorityPercentage;

    /** 
     * Full argument constructor comes second.
     */

    /**
     * Default constructor for ElectionConfiguration
     *
     * @param minSizeOfElectionGroup
     *            The minimum size of the election group required by the leader
     *            to start an election
     * @param minNumberOfConvergedNodes
     *            The minimum number of converged nodes who replies in a leader
     *            election
     * @param minPercentageOfVotes
     *            The minimum number of total messages to be sent back in an
     *            election
     * @param voteRequestTimeout
     *            The amount of time (ms) the leader waits for the electors to
     *            reply
     * @param heartbeatTimeoutInterval
     *            The interval (ms) of how often heart beat messages are being
     *            sent
     * @param heartbeatTimeoutDelay
     *            The time (ms) the leader waits before sending the first heart
     *            beat message
     * @param leaderDeathMajorityPercentage
     *            The percentage of alive votes required for the leader to be
     *            seen as alive
     * @param heartbeatWaitTimeout
     *            The amount of time (ms) a follower will wait for until his
     *            header's heart beat messages time out
     * @param rejectedTimeout
     *            The amount of time (ms) a follower will wait to receive a
     *            rejected message from the leader
     * @param deathTimeout
     *            The amount of time (ms) the caller of the death vote will wait
     *            for all messages to return
     * @param deathVoteMajorityPercentage
     *            The number of nodes that are needed to deem a leader to be
     *            dead
     * @param waitForNoOfIndexMessages
     *            The new leader will wait for this number of messages before
     *            announcing his leadership
     * @param indexTimeout
     *            The amount of time (ms) the new leader will wait for index
     *            messages to return
     * @param nodeSuggestion
     *            Whether the leader election should suggest nodes to TMan in
     *            case the view only has nodes with lower utility values. True
     *            if node suggestion should be turned on
     */
    /**
     * Default constructor comes first.
     */
    public ElectionConfiguration() {        
        this(MsConfig.getSeed(),
                MsConfig.ELECTION_INDEX_TIMEOUT,
                MsConfig.ELECTION_DEATH_TIMEOUT,
                MsConfig.ELECTION_REJECTED_TIMEOUT,
                MsConfig.ELECTION_NODE_SUGGESTION,
                MsConfig.ELECTION_VOTE_REQUEST_TIMEOUT ,
                MsConfig.ELECTION_HEARTBEAT_WAIT_TIMEOUT ,
                MsConfig.ELECTION_HEARTBEAT_TIMEOUTDELAY ,
                MsConfig.ELECTION_MIN_SIZE_ELECTIONGROUP ,
                MsConfig.ELECTION_MIN_PERCENTAGE_VOTES ,
                MsConfig.ELECTION_WAIT_FOR_NO_INDEX_MESSAGES ,
                MsConfig.ELECTION_HEARTBEAT_TIMEOUT_INTERVAL ,
                MsConfig.ELECTION_MIN_NUMBER_CONVERGED_NODES ,
                MsConfig.ELECTION_DEATH_VOTE_MAJORITY_PERCENTAGE ,
                MsConfig.ELECTION_LEADER_DEATH_MAJORITY_PERCENTAGE
                );
    }

    /**
     * Full argument constructor comes next
     */
    public ElectionConfiguration(int seed, int indexTimeout, int deathTimeout, int rejectedTimeout, boolean nodeSuggestion, 
            int voteRequestTimeout, int heartbeatWaitTimeout, int heartbeatTimeoutDelay, int minSizeOfElectionGroup, 
            double minPercentageOfVotes, int waitForNoOfIndexMessages, 
            int heartbeatTimeoutInterval, int minNumberOfConvergedNodes, double deathVoteMajorityPercentage, 
            double leaderDeathMajorityPercentage) {
        super(seed);
        this.indexTimeout = indexTimeout;
        this.deathTimeout = deathTimeout;
        this.rejectedTimeout = rejectedTimeout;
        this.nodeSuggestion = nodeSuggestion;
        this.voteRequestTimeout = voteRequestTimeout;
        this.heartbeatWaitTimeout = heartbeatWaitTimeout;
        this.heartbeatTimeoutDelay = heartbeatTimeoutDelay;
        this.minSizeOfElectionGroup = minSizeOfElectionGroup;
        this.minPercentageOfVotes = minPercentageOfVotes;
        this.waitForNoOfIndexMessages = waitForNoOfIndexMessages;
        this.heartbeatTimeoutInterval = heartbeatTimeoutInterval;
        this.minNumberOfConvergedNodes = minNumberOfConvergedNodes;
        this.deathVoteMajorityPercentage = deathVoteMajorityPercentage;
        this.leaderDeathMajorityPercentage = leaderDeathMajorityPercentage;
    }

    
    public static ElectionConfiguration build() {
        return new ElectionConfiguration();
    }

    public int getIndexTimeout() {
        return indexTimeout;
    }

    public ElectionConfiguration setIndexTimeout(int indexTimeout) {
        this.indexTimeout = indexTimeout;
        return this;
    }

    public int getDeathTimeout() {
        return deathTimeout;
    }

    public ElectionConfiguration setDeathTimeout(int deathTimeout) {
        this.deathTimeout = deathTimeout;
        return this;
    }

    public int getRejectedTimeout() {
        return rejectedTimeout;
    }

    public ElectionConfiguration setRejectedTimeout(int rejectedTimeout) {
        this.rejectedTimeout = rejectedTimeout;
        return this;
    }

    public boolean isNodeSuggestion() {
        return nodeSuggestion;
    }

    public ElectionConfiguration setNodeSuggestion(boolean nodeSuggestion) {
        this.nodeSuggestion = nodeSuggestion;
        return this;
    }

    public int getVoteRequestTimeout() {
        return voteRequestTimeout;
    }

    public ElectionConfiguration setVoteRequestTimeout(int voteRequestTimeout) {
        this.voteRequestTimeout = voteRequestTimeout;
        return this;
    }

    public int getHeartbeatWaitTimeout() {
        return heartbeatWaitTimeout;
    }

    public ElectionConfiguration setHeartbeatWaitTimeout(int heartbeatWaitTimeout) {
        this.heartbeatWaitTimeout = heartbeatWaitTimeout;
        return this;
    }

    public int getHeartbeatTimeoutDelay() {
        return heartbeatTimeoutDelay;
    }

    public ElectionConfiguration setHeartbeatTimeoutDelay(int heartbeatTimeoutDelay) {
        this.heartbeatTimeoutDelay = heartbeatTimeoutDelay;
        return this;
    }

    public int getMinSizeOfElectionGroup() {
        return minSizeOfElectionGroup;
    }

    public ElectionConfiguration setMinSizeOfElectionGroup(int minSizeOfElectionGroup) {
        this.minSizeOfElectionGroup = minSizeOfElectionGroup;
        return this;
    }

    public double getMinPercentageOfVotes() {
        return minPercentageOfVotes;
    }

    public ElectionConfiguration setMinPercentageOfVotes(double minPercentageOfVotes) {
        this.minPercentageOfVotes = minPercentageOfVotes;
        return this;
    }

    public int getWaitForNoOfIndexMessages() {
        return waitForNoOfIndexMessages;
    }

    public ElectionConfiguration setWaitForNoOfIndexMessages(int waitForNoOfIndexMessages) {
        this.waitForNoOfIndexMessages = waitForNoOfIndexMessages;
        return this;
    }

    public int getHeartbeatTimeoutInterval() {
        return heartbeatTimeoutInterval;
    }

    public ElectionConfiguration setHeartbeatTimeoutInterval(int heartbeatTimeoutInterval) {
        this.heartbeatTimeoutInterval = heartbeatTimeoutInterval;
        return this;
    }

    public int getMinNumberOfConvergedNodes() {
        return minNumberOfConvergedNodes;
    }

    public ElectionConfiguration setMinNumberOfConvergedNodes(int minNumberOfConvergedNodes) {
        this.minNumberOfConvergedNodes = minNumberOfConvergedNodes;
        return this;
    }

    public double getDeathVoteMajorityPercentage() {
        return deathVoteMajorityPercentage;
    }

    public ElectionConfiguration setDeathVoteMajorityPercentage(double deathVoteMajorityPercentage) {
        this.deathVoteMajorityPercentage = deathVoteMajorityPercentage;
        return this;
    }

    public double getLeaderDeathMajorityPercentage() {
        return leaderDeathMajorityPercentage;
    }

    public ElectionConfiguration setLeaderDeathMajorityPercentage(double leaderDeathMajorityPercentage) {
        this.leaderDeathMajorityPercentage = leaderDeathMajorityPercentage;
        return this;
    }
    

}
