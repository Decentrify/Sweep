package se.sics.ms.election;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import se.sics.gvod.common.Self;
import se.sics.gvod.common.msgs.RelayMsgNetty;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.net.msgs.RewriteableMsg;
import se.sics.gvod.net.msgs.ScheduleRetryTimeout;
import se.sics.gvod.timer.*;
import se.sics.kompics.*;
import se.sics.peersearch.messages.*;

import se.sics.ms.configuration.ElectionConfiguration;
import se.sics.ms.gradient.BroadcastGradientPartnersPort;
import se.sics.ms.gradient.BroadcastGradientPartnersPort.TmanPartners;
import se.sics.ms.gradient.LeaderStatusPort;
import se.sics.ms.gradient.LeaderStatusPort.LeaderStatusRequest;
import se.sics.ms.gradient.LeaderStatusPort.LeaderStatusResponse;
import se.sics.ms.gradient.LeaderStatusPort.NodeCrashEvent;

/**
 * This class contains functions for those nodes that are directly following a
 * leader. Such functions range from leader election to how to handle the
 * scenario when no more heart beat messages are received
 */
public class ElectionFollower extends ComponentDefinition {
	Positive<Timer> timerPort = positive(Timer.class);
	Positive<VodNetwork> networkPort = positive(VodNetwork.class);
	Negative<BroadcastGradientPartnersPort> tmanBroadCast = negative(BroadcastGradientPartnersPort.class);
	Positive<LeaderStatusPort> leaderStatusPort = positive(LeaderStatusPort.class);
	Negative<LeaderStatusPort> leaderStatusPortNeg = negative(LeaderStatusPort.class);

	private ElectionConfiguration electionConfiguration;
    private Self self;
	private VodAddress leader;
	private ArrayList<VodAddress> leaderView, higherNodes, lowerNodes;
	private boolean leaderIsAlive, isConverged;
	private UUID heartBeatTimeoscutId, deathVoteTimeout;
	private SynchronizedCounter aliveCounter, deathMessageCounter;


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
	Handler<ElectionMessage.Request> handleVotingRequest = new Handler<ElectionMessage.Request>() {
		@Override
		public synchronized void handle(ElectionMessage.Request event) {
			boolean candidateAccepted = true;
			VodAddress highestNode = findHighestNodeInView();

			// Don't vote yes unless the source has the lowest ID
			if (highestNode.getId() < event.getSource().getId()
					|| (leader != null && leader.getId() < event.getSource().getId())) {
				candidateAccepted = false;
			} else {
				highestNode = event.getVodSource();
			}

            ElectionMessage.Response response = new ElectionMessage.Response(self.getAddress(), event.getVodSource(), self.getId(),
                    event.getVodSource().getId(), event.getNextDest(), event.getTimeoutId(), RelayMsgNetty.Status.OK, event.getVoteID(), isConverged, candidateAccepted, highestNode);

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
	Handler<VotingResultMessage> handleVotingResult = new Handler<VotingResultMessage>() {
		@Override
		public void handle(VotingResultMessage event) {
			VodAddress lowestId = event.getVodSource();

			// ----------------------------------------
			// Checking which address has the lowest ID
			// ----------------------------------------

			if (leader != null) {
				if (event.getSource().getId() > leader.getId()) {
					lowestId = leader;
				} else {
					lowestId = event.getVodSource();
				}

			} // Only need to check nodes with higher ID than the leader
			if (higherNodes != null) {
				for (VodAddress addr : higherNodes) {
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
					acceptLeader(event.getVodSource(), Arrays.asList(event.getView()));
				} else {
					// reject this node
					rejectLeader(event.getVodSource(), lowestId);
				}
			}
			// The current leader has the lowest ID
			else if (event.getSource().getId() == leader.getId()
					&& leader.getId() == lowestId.getId()) {
				acceptLeader(leader, Arrays.asList(event.getView()));
			}
			// The current leader does NOT have the lowest ID
			else if (event.getSource().getId() == lowestId.getId()
					&& lowestId.getId() < leader.getId()) {
				rejectLeader(leader, event.getVodSource());
				acceptLeader(event.getVodSource(), Arrays.asList(event.getView()));
			}
			// The source is the leader, but it is not suited to lead
			else if (event.getSource().getId() == leader.getId()) {
				rejectLeader(leader, lowestId);
			}
			// Someone with a higher ID is sending heart beats
			else {
				rejectLeaderCandidate(event.getVodSource(), lowestId);
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
	Handler<HeartbeatMessage.RequestTimeout> handleHeartBeatTimeOut = new Handler<HeartbeatMessage.RequestTimeout>() {
		@Override
		public void handle(HeartbeatMessage.RequestTimeout event) {
			if (leaderIsAlive == true) {
				leaderIsAlive = false;

				RejectFollowerMessage.Request msg = new RejectFollowerMessage.Request(self.getAddress(), leader, UUID.nextUUID());
				trigger(msg, networkPort);
				triggerTimeout(electionConfiguration.getRejectedTimeout(), event.getMsg());
			} else if (leader != null) {
                ScheduleTimeout timeout = new ScheduleTimeout(
                        electionConfiguration.getDeathTimeout());
                deathVoteTimeout = (UUID)timeout.getTimeoutEvent().getTimeoutId();

				for (VodAddress addr : leaderView) {
                    LeaderSuspectionMessage.Request msg = new LeaderSuspectionMessage.Request(self.getAddress(), addr, self.getId(), addr.getId(), deathVoteTimeout, leader);
					trigger(msg, networkPort);
				}

                timeout.setTimeoutEvent(new DeathTimeout(timeout));
			}
		}
	};

	/**
	 * A handler that receives rejected confirmation messages from the leader.
	 * The node will reject the leader in case it has been kicked from the
	 * leader's view, and is therefore no longer in the voting group
	 */
	Handler<RejectFollowerMessage.Response> handleRejecttionConfirmation = new Handler<RejectFollowerMessage.Response>() {
		@Override
		public void handle(RejectFollowerMessage.Response event) {
			if (event.isInView() == true) {
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
	Handler<LeaderSuspectionMessage.Request> handleLeaderDeathMsg = new Handler<LeaderSuspectionMessage.Request>() {
		@Override
		public void handle(LeaderSuspectionMessage.Request event) {
			LeaderSuspectionMessage.Response msg;

			// Same leader
			if (leader != null && event.getLeader().getId() == leader.getId()) {
                msg = new LeaderSuspectionMessage.Response(self.getAddress(), event.getVodSource(), event.getClientId(), event.getRemoteId(), event.getNextDest(), event.getTimeoutId(), RelayMsgNetty.Status.OK, leaderIsAlive, leader);
			} else { // Different leaders O.o
                msg = new LeaderSuspectionMessage.Response(self.getAddress(), event.getVodSource(), event.getClientId(), event.getRemoteId(), event.getNextDest(), event.getTimeoutId(), RelayMsgNetty.Status.OK, false, leader);
			}

			trigger(msg, networkPort);
		}
	};

    /**
     * A handler that counts how many death responses have been received. If the
     * number of votes are the same as the number of nodes in the leader's view,
     * then it calls upon a vote count
     */
    Handler<LeaderSuspectionMessage.Response> handleLeaderDeathResponse = new Handler<LeaderSuspectionMessage.Response>() {
        @Override
        public void handle(LeaderSuspectionMessage.Response event) {
            if ((leader == null) || (event.getLeader() == null)
                    || (leader != null && leader.getId() != event.getLeader().getId())) {
                return;
            }

            deathMessageCounter.incrementValue();
            if (event.isSuspected() == true) {
                aliveCounter.incrementValue();
            }

            if (leaderView != null && deathMessageCounter.getValue() == leaderView.size()) {
                evaluateDeathResponses();
            }
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
	 * A handler that will set the leader to null in case the other nodes have
	 * confirmed the leader to be dead
	 */
	Handler<LeaderAnnouncementMessage> handleLeaderDeath = new Handler<LeaderAnnouncementMessage>() {
		@Override
		public void handle(LeaderAnnouncementMessage event) {
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

			for (VodAddress addr : leaderView) {
                LeaderAnnouncementMessage msg = new LeaderAnnouncementMessage(self.getAddress(), addr, leader);
				trigger(msg, networkPort);
			}

			trigger(new NodeCrashEvent(leader), leaderStatusPort);
			removeNodeFromView(leader);
			leader = null;
			leaderView = null;
		} else { // The leader MIGHT be alive
			// TODO maybe you got rejected?
			leaderIsAlive = true;
			triggerTimeout(null);
		}

		deathMessageCounter.setValue(0);
		aliveCounter.setValue(0);
	}

	/**
	 * Starts the next heart beat timeout using
	 * electionConfiguration.getHeartbeatWaitTimeout() as period
	 */
	private void triggerTimeout(RewriteableMsg message) {
		triggerTimeout(electionConfiguration.getHeartbeatWaitTimeout(), message);
	}

	/**
	 * Starts the next heart beat timeout using a custom period
	 * 
	 * @param timeOut
	 *            custom period in ms
	 */
	private void triggerTimeout(int timeOut, RewriteableMsg message) {
		cancelTimeout();

        ScheduleRetryTimeout st =
                new ScheduleRetryTimeout(timeOut,
                        0, 0);
        HeartbeatMessage.RequestTimeout request = new HeartbeatMessage.RequestTimeout(st, message);
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
	private void acceptLeader(VodAddress node, Collection<VodAddress> view) {
		leaderIsAlive = true;
		leader = node;
		leaderView = new ArrayList<VodAddress>(view);

		// Cancel old timeouts and create a new one
		triggerTimeout(null);
	}

	/**
	 * Rejects the leader
	 * 
	 * @param node
	 *            the leader's Addres
	 * @param betterNode
	 *            the better node's Address
	 */
	private void rejectLeader(VodAddress node, VodAddress betterNode) {
		leader = null;
		leaderView = null;
		leaderIsAlive = false;

		// Cancel old timeouts
		cancelTimeout();
		RejectLeaderMessage msg = new RejectLeaderMessage(self.getAddress(), node, betterNode);
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
	private void rejectLeaderCandidate(VodAddress node, VodAddress betterNode) {
        RejectLeaderMessage msg = new RejectLeaderMessage(self.getAddress(), node, betterNode);
		trigger(msg, networkPort);
	}

	/**
	 * Returns the node from this node's view that is farthest up the topology
	 * 
	 * @return the highest node's Address
	 */
	private VodAddress findHighestNodeInView() {
		VodAddress lowestNode = self.getAddress();
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
	private VodAddress findHighestNodeInList(ArrayList<VodAddress> list, VodAddress initValue) {
        VodAddress lowestNode = initValue;

		if (list != null) {
			for (VodAddress addr : list) {
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
	private void removeNodeFromView(VodAddress node) {
		higherNodes.remove(node);
		lowerNodes.remove(node);
	}
}
