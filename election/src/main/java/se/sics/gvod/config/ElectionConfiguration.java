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
	int deathTimeout;
	int rejectedTimeout;
	int voteRequestTimeout;
	int heartbeatWaitTimeout;
	int heartbeatTimeoutDelay;
	int minSizeOfElectionGroup;
	double minPercentageOfVotes;
	int heartbeatTimeoutInterval;
	int minNumberOfConvergedNodes;
	double deathVoteMajorityPercentage;
	double leaderDeathMajorityPercentage;

    /**
     * Default constructor comes first.
     */
    public ElectionConfiguration() {        
        this(
                MsConfig.ELECTION_DEATH_TIMEOUT,
                MsConfig.ELECTION_REJECTED_TIMEOUT,
                MsConfig.ELECTION_VOTE_REQUEST_TIMEOUT,
                MsConfig.ELECTION_HEARTBEAT_WAIT_TIMEOUT,
                MsConfig.ELECTION_HEARTBEAT_TIMEOUTDELAY,
                MsConfig.ELECTION_MIN_SIZE_ELECTIONGROUP,
                MsConfig.ELECTION_MIN_PERCENTAGE_VOTES,
                MsConfig.ELECTION_HEARTBEAT_TIMEOUT_INTERVAL,
                MsConfig.ELECTION_MIN_NUMBER_CONVERGED_NODES,
                MsConfig.ELECTION_DEATH_VOTE_MAJORITY_PERCENTAGE,
                MsConfig.ELECTION_LEADER_DEATH_MAJORITY_PERCENTAGE
                );
    }

    /**
     * Full argument constructor comes second.
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
     */
    public ElectionConfiguration(int deathTimeout, int rejectedTimeout,
            int voteRequestTimeout, int heartbeatWaitTimeout, int heartbeatTimeoutDelay, int minSizeOfElectionGroup,
            double minPercentageOfVotes, int heartbeatTimeoutInterval, int minNumberOfConvergedNodes,
            double deathVoteMajorityPercentage, double leaderDeathMajorityPercentage) {
        this.deathTimeout = deathTimeout;
        this.rejectedTimeout = rejectedTimeout;
        this.voteRequestTimeout = voteRequestTimeout;
        this.heartbeatWaitTimeout = heartbeatWaitTimeout;
        this.heartbeatTimeoutDelay = heartbeatTimeoutDelay;
        this.minSizeOfElectionGroup = minSizeOfElectionGroup;
        this.minPercentageOfVotes = minPercentageOfVotes;
        this.heartbeatTimeoutInterval = heartbeatTimeoutInterval;
        this.minNumberOfConvergedNodes = minNumberOfConvergedNodes;
        this.deathVoteMajorityPercentage = deathVoteMajorityPercentage;
        this.leaderDeathMajorityPercentage = leaderDeathMajorityPercentage;
    }

    
    public static ElectionConfiguration build() {
        return new ElectionConfiguration();
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
