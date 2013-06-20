package election.system.peer.election;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.CancelTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import tman.system.peer.tman.BroadcastTManPartnersPort;
import tman.system.peer.tman.BroadcastTManPartnersPort.TmanPartners;
import tman.system.peer.tman.LeaderStatusPort;
import tman.system.peer.tman.LeaderStatusPort.LeaderStatusRequest;
import tman.system.peer.tman.LeaderStatusPort.LeaderStatusResponse;
import tman.system.peer.tman.LeaderStatusPort.NodeCrashEvent;

import common.configuration.ElectionConfiguration;

import election.system.peer.election.VotingMsg.LeaderDeathMsg;
import election.system.peer.election.VotingMsg.LeaderDeathResponseMsg;
import election.system.peer.election.VotingMsg.RejectFollowerConfirmationMsg;
import election.system.peer.election.VotingMsg.RejectFollowerMsg;
import election.system.peer.election.VotingMsg.RejectLeaderMsg;
import election.system.peer.election.VotingMsg.TheLeaderIsDefinitelyConfirmedToBeDeadMsg;
import election.system.peer.election.VotingMsg.VotingRequestMsg;
import election.system.peer.election.VotingMsg.VotingResponseMsg;
import election.system.peer.election.VotingMsg.VotingResultMsg;

/**
 * This class contains functions for those nodes that are directly following a
 * leader. Such functions range from leader election to how to handle the
 * scenario when no more heart beat messages are received
 */
public class ElectionFollower extends ComponentDefinition {
	Positive<Timer> timerPort = positive(Timer.class);
	Positive<Network> networkPort = positive(Network.class);
	Negative<BroadcastTManPartnersPort> tmanBroadCast = negative(BroadcastTManPartnersPort.class);
	Positive<LeaderStatusPort> leaderStatusPort = positive(LeaderStatusPort.class);
	Negative<LeaderStatusPort> leaderStatusPortNeg = negative(LeaderStatusPort.class);

	private ElectionConfiguration electionConfiguration;
	private Address leader, self;
	private ArrayList<Address> leaderView, higherNodes, lowerNodes;
	private boolean leaderIsAlive, isConverged;
	private UUID heartBeatTimeoscutId, deathVoteTimeout;
	private SynchronizedCounter aliveCounter, deathMessageCounter;

	/**
	 * A customised timeout class used for checking when heart beat messages
	 * should arrive
	 */
	public class HeartBeatSchedule extends Timeout {

		public HeartBeatSchedule(ScheduleTimeout request) {
			super(request);
		}

		public HeartBeatSchedule(SchedulePeriodicTimeout request) {
			super(request);
		}
	}

	/**
	 * A customised timeout class used to determine how long a node should wait
	 * for other nodes to reply to leader death messagew
	 */
	public class DeathTimeout extends Timeout {

		public DeathTimeout(ScheduleTimeout request) {
			super(request);
		}

		public DeathTimeout(SchedulePeriodicTimeout request) {
			super(request);
		}
	}

	/**
	 * Default constructor that subscribes certain handlers to ports
	 */
	public ElectionFollower() {
		subscribe(handleInit, control);
		subscribe(handleDeathTimeout, timerPort);
		subscribe(handleLeaderDeath, networkPort);
		subscribe(handleVotingResult, networkPort);
		subscribe(handleVotingRequest, networkPort);
		subscribe(handleLeaderDeathMsg, networkPort);
		subscribe(handleHeartBeatTimeOut, timerPort);
		subscribe(handleTManBroadcast, tmanBroadCast);
		subscribe(handleLeaderDeathResponse, networkPort);
		subscribe(handleRejecttionConfirmation, networkPort);
		subscribe(handleLeaderStatusRequest, leaderStatusPortNeg);
	}

	/**
	 * The initialisation handler. Called when the component is created and
	 * mainly initiates variables
	 */
	Handler<ElectionInit> handleInit = new Handler<ElectionInit>() {
		@Override
		public void handle(ElectionInit init) {
			self = init.getSelf();
			electionConfiguration = init.getElectionConfiguration();

			leader = null;
			leaderView = null;
			lowerNodes = null;
			higherNodes = null;

			aliveCounter = new SynchronizedCounter();
			deathMessageCounter = new SynchronizedCounter();
		}
	};

	/**
	 * A handler that will respond to voting requests sent from leader
	 * candidates. It checks if that leader candidate is a suitable leader
	 */
	Handler<VotingRequestMsg> handleVotingRequest = new Handler<VotingRequestMsg>() {
		@Override
		public synchronized void handle(VotingRequestMsg event) {
			boolean candidateAccepted = true;
			VotingResponseMsg response = new VotingResponseMsg(isConverged, event.getVoteID(),
					event.getRequestId(), event.getDestination(), event.getSource());
			Address highestNode = findHighestNodeInView();

			// Don't vote yes unless the source has the lowest ID
			if (highestNode.getId() < event.getSource().getId()
					|| (leader != null && leader.getId() < event.getSource().getId())) {
				candidateAccepted = false;
			} else {
				highestNode = event.getSource();
			}

			response.setVote(candidateAccepted);
			response.setHighestNode(highestNode);
			trigger(response, networkPort);
		}
	};

	/**
	 * A handler receiving TMan view broadcasts, and sets its view accordingly
	 */
	Handler<TmanPartners> handleTManBroadcast = new Handler<TmanPartners>() {
		@Override
		public void handle(TmanPartners event) {
			isConverged = event.isConverged();
			lowerNodes = event.getLowerNodes();
			higherNodes = event.getHigherNodes();
		}
	};

	/**
	 * Handler for receiving of heart beat messages from the leader. It checks
	 * if that node is still a suitable leader and handles the situation
	 * accordingly
	 */
	Handler<VotingResultMsg> handleVotingResult = new Handler<VotingResultMsg>() {
		@Override
		public void handle(VotingResultMsg event) {
			Address lowestId = event.getSource();

			// ----------------------------------------
			// Checking which address has the lowest ID
			// ----------------------------------------

			if (leader != null) {
				if (event.getSource().getId() > leader.getId()) {
					lowestId = leader;
				} else {
					lowestId = event.getSource();
				}

			} // Only need to check nodes with higher ID than the leader
			if (higherNodes != null) {
				for (Address addr : higherNodes) {
					if (addr.getId() < lowestId.getId()) {
						lowestId = addr;
					}
				}
			}

			// ---------------
			// Choose a leader
			// ---------------

			// There is no current leader, this SHOULD not happen
			if (leader == null) {
				// The requester has the lowest ID
				if (event.getSource().getId() == lowestId.getId()) {
					// Accept this node
					acceptLeader(event.getSource(), event.getLeaderView());
				} else {
					// reject this node
					rejectLeader(event.getSource(), lowestId);
				}
			}
			// The current leader has the lowest ID
			else if (event.getSource().getId() == leader.getId()
					&& leader.getId() == lowestId.getId()) {
				acceptLeader(leader, event.getLeaderView());
			}
			// The current leader does NOT have the lowest ID
			else if (event.getSource().getId() == lowestId.getId()
					&& lowestId.getId() < leader.getId()) {
				rejectLeader(leader, event.getSource());
				acceptLeader(event.getSource(), event.getLeaderView());
			}
			// The source is the leader, but it is not suited to lead
			else if (event.getSource().getId() == leader.getId()) {
				rejectLeader(leader, lowestId);
			}
			// Someone with a higher ID is sending heart beats
			else {
				rejectLeaderCandidate(event.getSource(), lowestId);
			}
		}
	};

	/**
	 * A handler responsible for the actions taken when the node has not
	 * received a heart beat message from the leader for a certain amount of
	 * time. First it will try to ask the leader if it has been kicked out of
	 * the leader's view. If there is no response from the leader it will call
	 * for a leader death vote. It won't call for a leader death vote right away
	 * in case the local version of the leaver's view is completely outdated,
	 * because that could result in valid leaders being rejected
	 */
	Handler<HeartBeatSchedule> handleHeartBeatTimeOut = new Handler<HeartBeatSchedule>() {
		@Override
		public void handle(HeartBeatSchedule event) {
			if (leaderIsAlive == true) {
				leaderIsAlive = false;

				RejectFollowerMsg msg = new RejectFollowerMsg(self, leader);
				trigger(msg, networkPort);
				triggerTimeout(electionConfiguration.getRejectedTimeout());
			} else if (leader != null) {
				LeaderDeathMsg msg = null;

				for (Address addr : leaderView) {
					msg = new LeaderDeathMsg(leader, self, addr);
					trigger(msg, networkPort);
				}

				ScheduleTimeout timeout = new ScheduleTimeout(
						electionConfiguration.getDeathTimeout());
				timeout.setTimeoutEvent(new DeathTimeout(timeout));
				deathVoteTimeout = timeout.getTimeoutEvent().getTimeoutId();
			}
		}
	};

	/**
	 * A handler that receives rejected confirmation messages from the leader.
	 * The node will reject the leader in case it has been kicked from the
	 * leader's view, and is therefore no longer in the voting group
	 */
	Handler<RejectFollowerConfirmationMsg> handleRejecttionConfirmation = new Handler<RejectFollowerConfirmationMsg>() {
		@Override
		public void handle(RejectFollowerConfirmationMsg event) {
			if (event.isNodeInView() == true) {
				leaderIsAlive = true;
			} else {
				leader = null;
				leaderView = null;
				leaderIsAlive = false;

				cancelTimeout();
			}
		}
	};

	/**
	 * A handler that will respond whether it thinks that the leader is dead or
	 * not
	 */
	Handler<LeaderDeathMsg> handleLeaderDeathMsg = new Handler<LeaderDeathMsg>() {
		@Override
		public void handle(LeaderDeathMsg event) {
			LeaderDeathResponseMsg msg = null;

			// Same leader
			if (leader != null && event.getLeader().getId() == leader.getId()) {
				msg = new LeaderDeathResponseMsg(leader, leaderIsAlive, self, event.getSource());
			} else { // Different leaders O.o
				msg = new LeaderDeathResponseMsg(leader, false, self, event.getSource());
			}

			trigger(msg, networkPort);
		}
	};

	/**
	 * A handler that listens for DeathTimeout event and will then call for an
	 * evaluation of death responses
	 */
	Handler<DeathTimeout> handleDeathTimeout = new Handler<ElectionFollower.DeathTimeout>() {
		@Override
		public void handle(DeathTimeout event) {
			evaluateDeathResponses();
		}
	};

	/**
	 * A handler that counts how many death responses have been received. If the
	 * number of votes are the same as the number of nodes in the leader's view,
	 * then it calls upon a vote count
	 */
	Handler<LeaderDeathResponseMsg> handleLeaderDeathResponse = new Handler<LeaderDeathResponseMsg>() {
		@Override
		public void handle(LeaderDeathResponseMsg event) {
			if ((leader == null) || (event.getLeader() == null)
					|| (leader != null && leader.getId() != event.getLeader().getId())) {
				return;
			}

			deathMessageCounter.incrementValue();
			if (event.isLeaderAlive() == true) {
				aliveCounter.incrementValue();
			}

			if (leaderView != null && deathMessageCounter.getValue() == leaderView.size()) {
				evaluateDeathResponses();
			}
		}
	};

	/**
	 * A handler that will set the leader to null in case the other nodes have
	 * confirmed the leader to be dead
	 */
	Handler<TheLeaderIsDefinitelyConfirmedToBeDeadMsg> handleLeaderDeath = new Handler<TheLeaderIsDefinitelyConfirmedToBeDeadMsg>() {
		@Override
		public void handle(TheLeaderIsDefinitelyConfirmedToBeDeadMsg event) {
			if (leader != null && event.getLeader().getId() == leader.getId()) {
				// cancel timeout and reset
				cancelTimeout();
				trigger(new NodeCrashEvent(leader), leaderStatusPort);
				removeNodeFromView(leader);

				leader = null;
				leaderView = null;
				leaderIsAlive = false;
			}
		}
	};

	/**
	 * A handler that returns the status of the current leader
	 */
	Handler<LeaderStatusRequest> handleLeaderStatusRequest = new Handler<LeaderStatusPort.LeaderStatusRequest>() {
		@Override
		public void handle(LeaderStatusRequest event) {
			trigger(new LeaderStatusResponse(leader), leaderStatusPortNeg);
		}
	};

	/**
	 * Counts the votes from its leader death election
	 */
	private void evaluateDeathResponses() {

		CancelTimeout timeout = new CancelTimeout(deathVoteTimeout);
		trigger(timeout, timerPort);

		// The leader is considered dead
		if (leader != null
				&& leaderView != null
				&& deathMessageCounter.getValue() >= leaderView.size()
						* electionConfiguration.getDeathVoteMajorityPercentage()
				&& aliveCounter.getValue() < Math.ceil((float) leaderView.size()
						* electionConfiguration.getLeaderDeathMajorityPercentage())) {

			TheLeaderIsDefinitelyConfirmedToBeDeadMsg msg = null;

			for (Address addr : leaderView) {
				msg = new TheLeaderIsDefinitelyConfirmedToBeDeadMsg(leader, self, addr);
				trigger(msg, networkPort);
			}

			trigger(new NodeCrashEvent(leader), leaderStatusPort);
			removeNodeFromView(leader);
			leader = null;
			leaderView = null;
		} else { // The leader MIGHT be alive
			// TODO maybe you got rejected?
			leaderIsAlive = true;
			triggerTimeout();
		}

		deathMessageCounter.setValue(0);
		aliveCounter.setValue(0);
	}

	/**
	 * Starts the next heart beat timeout using
	 * electionConfiguration.getHeartbeatWaitTimeout() as period
	 */
	private void triggerTimeout() {
		triggerTimeout(electionConfiguration.getHeartbeatWaitTimeout());
	}

	/**
	 * Starts the next heart beat timeout using a custom period
	 * 
	 * @param timeOut
	 *            custom period in ms
	 */
	private void triggerTimeout(int timeOut) {
		cancelTimeout();

		ScheduleTimeout tOut = new ScheduleTimeout(timeOut);
		tOut.setTimeoutEvent(new HeartBeatSchedule(tOut));
		heartBeatTimeoscutId = tOut.getTimeoutEvent().getTimeoutId();
		trigger(tOut, timerPort);
	}

	/**
	 * Cancels the heart beat timeout
	 */
	private void cancelTimeout() {
		CancelTimeout ct = new CancelTimeout(heartBeatTimeoscutId);
		trigger(ct, timerPort);
	}

	/**
	 * Accepts a certain leader
	 * 
	 * @param node
	 *            the leader's Address
	 * @param view
	 *            the leader's current view
	 */
	private void acceptLeader(Address node, Collection<Address> view) {
		leaderIsAlive = true;
		leader = node;
		leaderView = new ArrayList<Address>(view);

		// Cancel old timeouts and create a new one
		triggerTimeout();
	}

	/**
	 * Rejects the leader
	 * 
	 * @param node
	 *            the leader's Addres
	 * @param betterNode
	 *            the better node's Address
	 */
	private void rejectLeader(Address node, Address betterNode) {
		leader = null;
		leaderView = null;
		leaderIsAlive = false;

		// Cancel old timeouts
		cancelTimeout();
		RejectLeaderMsg msg = new RejectLeaderMsg(betterNode, self, node);
		trigger(msg, networkPort);
	}

	/**
	 * Rejects a leader candidate
	 * 
	 * @param node
	 *            the leader candidate's Address
	 * @param betterNode
	 *            the better node's Address
	 */
	private void rejectLeaderCandidate(Address node, Address betterNode) {
		RejectLeaderMsg msg = new RejectLeaderMsg(betterNode, self, node);
		trigger(msg, networkPort);
	}

	/**
	 * Returns the node from this node's view that is farthest up the topology
	 * 
	 * @return the highest node's Address
	 */
	private Address findHighestNodeInView() {
		Address lowestNode = self;
		lowestNode = findHighestNodeInList(lowerNodes, lowestNode);
		lowestNode = findHighestNodeInList(higherNodes, lowestNode);

		return lowestNode;
	}

	// Highest as in farthest up
	/**
	 * Compares all the nodes in a list and returns the value with the highest
	 * utility value, ie. the node that is farthest up the topology tree
	 * 
	 * @param list
	 *            the list of Addresses that is going to be compared
	 * @param initValue
	 *            the Address of the initial value
	 * @return the node with the highest utility value
	 */
	private Address findHighestNodeInList(ArrayList<Address> list, Address initValue) {
		Address lowestNode = initValue;

		if (list != null) {
			for (Address addr : list) {
				if (addr.getId() < lowestNode.getId()) {
					lowestNode = addr;
				}
			}
		}

		return lowestNode;
	}

	/**
	 * Removes a specific node from the view
	 * 
	 * @param node
	 *            the node that is to be removed
	 */
	private void removeNodeFromView(Address node) {
		higherNodes.remove(node);
		lowerNodes.remove(node);
	}
}
