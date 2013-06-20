package common.configuration;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.Properties;

/**
 * This class has the functionality to store configuration parameters to disc as
 * well as retrieve them and store them in memory
 */
public class ElectionConfiguration {
	private final int indexTimeout;
	private final int deathTimeout;
	private final int rejectedTimeout;
	private final boolean nodeSuggestion;
	private final int voteRequestTimeout;
	private final int heartbeatWaitTimeout;
	private final int heartbeatTimeoutDelay;
	private final int minSizeOfElectionGroup;
	private final double minPercentageOfVotes;
	private final int waitForNoOfIndexMessages;
	private final int heartbeatTimeoutInterval;
	private final int minNumberOfConvergedNodes;
	private final double deathVoteMajorityPercentage;
	private final double leaderDeathMajorityPercentage;

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
	public ElectionConfiguration(int minSizeOfElectionGroup, int minNumberOfConvergedNodes,
			double minPercentageOfVotes, int voteRequestTimeout, int heartbeatTimeoutInterval,
			int heartbeatTimeoutDelay, double leaderDeathMajorityPercentage,
			int heartbeatWaitTimeout, int rejectedTimeout, int deathTimeout,
			double deathVoteMajorityPercentage, int waitForNoOfIndexMessages, int indexTimeout,
			boolean nodeSuggestion) {
		super();
		this.indexTimeout = indexTimeout;
		this.deathTimeout = deathTimeout;
		this.nodeSuggestion = nodeSuggestion;
		this.rejectedTimeout = rejectedTimeout;
		this.voteRequestTimeout = voteRequestTimeout;
		this.minPercentageOfVotes = minPercentageOfVotes;
		this.heartbeatWaitTimeout = heartbeatWaitTimeout;
		this.heartbeatTimeoutDelay = heartbeatTimeoutDelay;
		this.minSizeOfElectionGroup = minSizeOfElectionGroup;
		this.waitForNoOfIndexMessages = waitForNoOfIndexMessages;
		this.heartbeatTimeoutInterval = heartbeatTimeoutInterval;
		this.minNumberOfConvergedNodes = minNumberOfConvergedNodes;
		this.deathVoteMajorityPercentage = deathVoteMajorityPercentage;
		this.leaderDeathMajorityPercentage = leaderDeathMajorityPercentage;
	}

	/**
	 * Returns the amount of time (ms) the new leader will wait for index
	 * messages to return
	 * 
	 * @return int - indexTimeout
	 */
	public int getIndexTimeout() {
		return indexTimeout;
	}

	/**
	 * Returns the amount of time (ms) the caller of the death vote will wait
	 * for all messages to return
	 * 
	 * @return int - deathTimeout
	 */
	public int getDeathTimeout() {
		return deathTimeout;
	}

	/**
	 * Returns the amount of time (ms) a follower will wait to receive a
	 * rejected message from the leader
	 * 
	 * @return int - rejectedTimeout
	 */
	public int getRejectedTimeout() {
		return rejectedTimeout;
	}

	/**
	 * Getter for the node suggestion
	 * 
	 * @return true if node suggestion should be turned on
	 */
	public boolean getNodeSuggestion() {
		return this.nodeSuggestion;
	}

	/**
	 * Returns the amount of time (ms) the leader waits for the electors to
	 * reply
	 * 
	 * @return int - voteRequestTimeout
	 */
	public int getVoteRequestTimeout() {
		return voteRequestTimeout;
	}

	/**
	 * Returns the amount of time (ms) a follower will wait for until his
	 * header's heart beat messages time out
	 * 
	 * @return int - heartbeatWaitTimeout
	 */
	public int getHeartbeatWaitTimeout() {
		return heartbeatWaitTimeout;
	}

	/**
	 * Returns the time (ms) the leader waits before sending the first heart
	 * beat message
	 * 
	 * @return int - heartbeatTimeoutDelay
	 */
	public int getHeartbeatTimeoutDelay() {
		return heartbeatTimeoutDelay;
	}

	/**
	 * Returns the minimum size of the election group required by the leader to
	 * start an election
	 * 
	 * @return int - minSizeOfElectionGroup
	 */
	public int getMinSizeOfElectionGroup() {
		return minSizeOfElectionGroup;
	}

	/**
	 * Returns the minimum number of total messages to be sent back in an
	 * election
	 * 
	 * @return double - minPercentageOfVotes
	 */
	public double getMinPercentageOfVotes() {
		return minPercentageOfVotes;
	}

	/**
	 * Returns the new leader will wait for this number of messages before
	 * announcing his leadership
	 * 
	 * @return int - waitForNoOfIndexMessages
	 */
	public int getWaitForNoOfIndexMessages() {
		return waitForNoOfIndexMessages;
	}

	/**
	 * Returns the interval (ms) of how often heart beat messages are being sent
	 * 
	 * @return int - heartbeatTimeoutInterval
	 */
	public int getHeartbeatTimeoutInterval() {
		return heartbeatTimeoutInterval;
	}

	/**
	 * Returns the minimum number of converged nodes who replies in a leader
	 * election
	 * 
	 * @return int - minNumberOfConvergedNodes
	 */
	public int getMinNumberOfConvergedNodes() {
		return minNumberOfConvergedNodes;
	}

	/**
	 * The number of nodes that are needed to deem a leader to be dead
	 * 
	 * @return - double deathVoteMajorityPercentage
	 */
	public double getDeathVoteMajorityPercentage() {
		return deathVoteMajorityPercentage;
	}

	/**
	 * Returns the percentage of alive votes required for the leader to be seen
	 * as alive
	 * 
	 * @return double - leaderDeathMajorityPercentage
	 */
	public double getLeaderDeathMajorityPercentage() {
		return leaderDeathMajorityPercentage;
	}

	/**
	 * Creates a storage file containing the parameters and their values
	 * 
	 * @param file
	 *            Name of the file where the parameters will be stored
	 * @throws IOException
	 *             Is thrown in case the write does not succeed
	 */
	public void store(String file) throws IOException {
		Properties p = new Properties();
		p.setProperty("indexTimeout", "" + indexTimeout);
		p.setProperty("deathTimeout", "" + deathTimeout);
		p.setProperty("nodeSuggestion", "" + nodeSuggestion);
		p.setProperty("rejectedTimeout", "" + rejectedTimeout);
		p.setProperty("voteRequestTimeout", "" + voteRequestTimeout);
		p.setProperty("heartbeatWaitTimeout", "" + heartbeatWaitTimeout);
		p.setProperty("minPercentageOfVotes", "" + minPercentageOfVotes);
		p.setProperty("heartbeatTimeoutDelay", "" + heartbeatTimeoutDelay);
		p.setProperty("minSizeOfElectionGroup", "" + minSizeOfElectionGroup);
		p.setProperty("waitForNoOfIndexMessages", "" + waitForNoOfIndexMessages);
		p.setProperty("heartbeatTimeoutInterval", "" + heartbeatTimeoutInterval);
		p.setProperty("minNumberOfConvergedNodes", "" + minNumberOfConvergedNodes);
		p.setProperty("deathVoteMajorityPercentage", "" + deathVoteMajorityPercentage);
		p.setProperty("leaderDeathMajorityPercentage", "" + leaderDeathMajorityPercentage);

		Writer writer = new FileWriter(file);
		p.store(writer, "se.sics.kompics.p2p.overlay.application");
	}

	/**
	 * Loads the configurations from a configuration file
	 * 
	 * @param file
	 *            the name of the file used for loading the configurations
	 * @return a new instance of an ElectionConfiguration loaded with the
	 *         parameters read from the file
	 * @throws IOException
	 *             is thrown in case the read from file does not succeed
	 */
	public static ElectionConfiguration load(String file) throws IOException {
		Properties p = new Properties();
		Reader reader = new FileReader(file);
		p.load(reader);

		int indexTimeout = Integer.parseInt(p.getProperty("indexTimeout"));
		int deathTimeout = Integer.parseInt(p.getProperty("deathTimeout"));
		int rejectedTimeout = Integer.parseInt(p.getProperty("rejectedTimeout"));
		boolean nodeSuggestion = Boolean.parseBoolean(p.getProperty("nodeSuggestion"));
		int voteRequestTimeout = Integer.parseInt(p.getProperty("voteRequestTimeout"));
		int heartbeatWaitTimeout = Integer.parseInt(p.getProperty("heartbeatWaitTimeout"));
		int heartbeatTimeoutDelay = Integer.parseInt(p.getProperty("heartbeatTimeoutDelay"));
		int minSizeOfElectionGroup = Integer.parseInt(p.getProperty("minSizeOfElectionGroup"));
		double minPercentageOfVotes = Double.parseDouble(p.getProperty("minPercentageOfVotes"));
		int waitForNoOfIndexMessages = Integer.parseInt(p.getProperty("waitForNoOfIndexMessages"));
		int heartbeatTimeoutInterval = Integer.parseInt(p.getProperty("heartbeatTimeoutInterval"));
		int minNumberOfConvergedNodes = Integer
				.parseInt(p.getProperty("minNumberOfConvergedNodes"));
		double deathVoteMajorityPercentage = Double.parseDouble(p
				.getProperty("deathVoteMajorityPercentage"));
		double leaderDeathMajorityPercentage = Double.parseDouble(p
				.getProperty("leaderDeathMajorityPercentage"));

		return new ElectionConfiguration(minSizeOfElectionGroup, minNumberOfConvergedNodes,
				minPercentageOfVotes, voteRequestTimeout, heartbeatTimeoutInterval,
				heartbeatTimeoutDelay, leaderDeathMajorityPercentage, heartbeatWaitTimeout,
				rejectedTimeout, deathTimeout, deathVoteMajorityPercentage,
				waitForNoOfIndexMessages, indexTimeout, nodeSuggestion);
	}
}
