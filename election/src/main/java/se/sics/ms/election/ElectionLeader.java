package se.sics.ms.election;

import se.sics.gvod.common.Self;
import se.sics.gvod.config.ElectionConfiguration;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.timer.*;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.ms.gradient.BroadcastGradientPartnersPort;
import se.sics.ms.gradient.BroadcastGradientPartnersPort.GradientPartners;
import se.sics.ms.gradient.IndexRoutingPort;
import se.sics.ms.gradient.LeaderStatusPort;
import se.sics.ms.gradient.LeaderStatusPort.LeaderStatus;
import se.sics.ms.gradient.LeaderStatusPort.LeaderStatusRequest;
import se.sics.ms.gradient.LeaderStatusPort.LeaderStatusResponse;
import se.sics.ms.gradient.LeaderStatusPort.NodeSuggestion;
import se.sics.ms.snapshot.Snapshot;
import se.sics.peersearch.messages.*;

import java.util.ArrayList;

/**
 * This component contains functions for how a node will find out if it is the
 * leader. It also contains functions that a leader needs in order to perform
 * leader elections etc.
 */
public class ElectionLeader extends ComponentDefinition {
	Positive<Timer> timerPort = positive(Timer.class);
	Positive<VodNetwork> networkPort = positive(VodNetwork.class);
	Negative<BroadcastGradientPartnersPort> broadcast = negative(BroadcastGradientPartnersPort.class);
	Positive<LeaderStatusPort> leaderStatusPort = positive(LeaderStatusPort.class);
	Negative<IndexRoutingPort> indexRoutingPort = negative(IndexRoutingPort.class);

	private ElectionConfiguration config;
	private int numberOfNodesAtVotingTime;
	private int yesVotes, totalVotes, electionCounter, convergedCounter,
			indexMessageCounter;
	private boolean electionInProgress, iAmLeader, allowingIndexMessages;
	private Self self;
	private ArrayList<VodAddress> lowerNodes, higherNodes;
	private TimeoutId scheduledTimeoutId, voteTimeout, indexMsgTimeoutId, indexMessageID;

	/**
	 * A customised timeout class for when to send heart beats etc
	 */
	public class ElectionSchedule extends Timeout {

		public ElectionSchedule(ScheduleTimeout request) {
			super(request);
		}

		public ElectionSchedule(SchedulePeriodicTimeout request) {
			super(request);
		}
	}

	public class VoteTimeout extends Timeout {

		public VoteTimeout(ScheduleTimeout request) {
			super(request);
		}

		public VoteTimeout(SchedulePeriodicTimeout request) {
			super(request);
		}
	}

	/**
	 * A customised timeout class used to time how long the leader is going to
	 * collect index ID values from other nodes
	 */
	public class LeaderTimeout extends Timeout {

		public LeaderTimeout(SchedulePeriodicTimeout request) {
			super(request);
		}

		public LeaderTimeout(ScheduleTimeout request) {
			super(request);
		}
	}

	/**
	 * Default constructor that initiates all the event subscriptions to
	 * handlers
	 */
	public ElectionLeader() {
		subscribe(handleInit, control);
		subscribe(handleHeartBeats, timerPort);
		subscribe(handleLeaderTimer, timerPort);
		subscribe(handleVoteTimeout, timerPort);
		subscribe(handleIndexResponse, networkPort);
		subscribe(handleVotingResponse, networkPort);
		subscribe(handleLeaderRejection, networkPort);
		subscribe(handleGradientBroadcast, broadcast);
		subscribe(handleRejectedFollower, networkPort);
		subscribe(handleLeaderStatusResponse, leaderStatusPort);
	}

	/**
	 * The initialisation handler. It is called when the component is loaded and
	 * will initiate variables
	 */
	Handler<ElectionInit> handleInit = new Handler<ElectionInit>() {
		@Override
		public void handle(ElectionInit init) {
			self = init.getSelf();
			config = init.getConfig();

			iAmLeader = false;
			electionInProgress = false;
			allowingIndexMessages = false;

			lowerNodes = new ArrayList<VodAddress>();
			higherNodes = new ArrayList<VodAddress>();
		}
	};

	/**
	 * Handler for the periodic Gradient views that are being sent. It checks if the
	 * node fulfills the requirements in order to become a leader, and in that
	 * case it will call for a leader election
	 */
	Handler<GradientPartners> handleGradientBroadcast = new Handler<GradientPartners>() {
		@Override
		public void handle(GradientPartners event) {
			higherNodes = event.getHigherNodes();
			lowerNodes = event.getLowerNodes();

			// Create view for Snapshot
			StringBuilder builder = new StringBuilder();
			for (VodAddress node : higherNodes) {
				builder.append(node.getId() + " ");
			}
			for (VodAddress node : lowerNodes) {
				builder.append(node.getId() + " ");
			}
			Snapshot.setCurrentView(self.getAddress(), builder.toString());

			if (event.isConverged()
					&& !iAmLeader
					&& !electionInProgress
					&& event.getHigherNodes().size() == 0
					&& event.getLowerNodes().size() >= config
							.getMinSizeOfElectionGroup()) {

				// Check if there is already a leader - GOTO
				// handleLeaderStatusResponse
				trigger(new LeaderStatusRequest(), leaderStatusPort);
			}
		}
	};

	/**
	 * A handler that counts the number of votes received from the followers. If
	 * all nodes have responded it will call for vote counting
	 */
	Handler<ElectionMessage.Response> handleVotingResponse = new Handler<ElectionMessage.Response>() {
		@Override
		public void handle(ElectionMessage.Response event) {
			// Check if the vote comes from this batch of votes
			if (electionCounter == event.getVoteId()) {
				totalVotes++;
				if (event.isVote() == true) {
					yesVotes++;
				} else {
					// Rejected because there is a node above me
				}
				if (event.isConvereged() == true) {
					convergedCounter++;
				}
			}

			// Reject if there is a no-vote
			if (totalVotes != yesVotes) {
				rejected(event.getVodSource(), event.getHighest());
			}
			// Count the votes if if all votes have returned
			else if (totalVotes >= numberOfNodesAtVotingTime) {
				countVotes();
			}
		}
	};

	/**
	 * A handler that will call for a vote call after a certain amount of time
	 * if not all voters have returned with a vote
	 */
	Handler<VoteTimeout> handleVoteTimeout = new Handler<VoteTimeout>() {
		@Override
		public void handle(VoteTimeout event) {
			countVotes();
		}
	};

	/**
	 * A handler that will periodically send out heart beats to the node's
	 * (leader's) followers
	 */
	Handler<ElectionSchedule> handleHeartBeats = new Handler<ElectionSchedule>() {
		@Override
		public void handle(ElectionSchedule event) {
			VodAddress lowestId = self.getAddress();

			// It is assumed that the list doesn't have to be sorted
			// Looks for the node with the lowest ID
			for (VodAddress addr : higherNodes) {
				if (addr.getId() < lowestId.getId()) {
					lowestId = addr;
				}
			}

			// IF this node is still the leader then send heart beats
			if (self.getId() == lowestId.getId() && iAmLeader == true) {
				sendLeaderView();
			} else {
				scheduledTimeoutId = event.getTimeoutId();
				rejected(self.getAddress(), lowestId);
			}
		}
	};

	/**
	 * A handler that handles rejected messages send by nodes who have found a
	 * better leader
	 */
	Handler<RejectLeaderMessage> handleLeaderRejection = new Handler<RejectLeaderMessage>() {
		@Override
		public void handle(RejectLeaderMessage event) {
			rejected(event.getVodSource(), event.getBetterLeader());
		}
	};

	/**
	 * A handler that handles nodes who have been kicked out of the leader's
	 * view and ask if the leader if still alive
	 */
	Handler<RejectFollowerMessage.Request> handleRejectedFollower = new Handler<RejectFollowerMessage.Request>() {
		@Override
		public void handle(RejectFollowerMessage.Request event) {
			boolean sourceIsInView = false;

			// Checks if the node is still in the leader's view
			for (VodAddress addr : lowerNodes) {
				if (addr.getId() == event.getVodSource().getId()) {
					sourceIsInView = true;
					break;
				}
			}

            RejectFollowerMessage.Response msg = new RejectFollowerMessage.Response(self.getAddress(), event.getVodSource(), UUID.nextUUID(), sourceIsInView);
			trigger(msg, networkPort);
		}
	};

	/**
	 * A handler that receives messages containing other nodes' highest index
	 * IDs and forwards them to Search. If a certain number of messages have
	 * been received it will ignore the rest and announce its leadership
	 */
	Handler<IndexResponseMessage> handleIndexResponse = new Handler<IndexResponseMessage>() {
		@Override
		public void handle(IndexResponseMessage event) {
			// Make sure that only recent messages are checked
			if (allowingIndexMessages == true && event.getMessageId().equals(indexMessageID)) {
				// Increase the counter and send the update to search
				indexMessageCounter++;
//				trigger(new IndexDisseminationEvent(event.getIndex()), indexRoutingPort);

				// When enough messages are received
				if (indexMessageCounter >= config
						.getWaitForNoOfIndexMessages()) {
					finishIndexMsgReading();
				}
			}
		}
	};

	/**
	 * A handler that announces the node's leadership if not enough index ID
	 * messages have been received within a certain amount of time
	 */
	Handler<LeaderTimeout> handleLeaderTimer = new Handler<LeaderTimeout>() {
		@Override
		public void handle(LeaderTimeout event) {
			finishIndexMsgReading();
		}
	};

	/**
	 * A handler that checks whether the node already has a leader and if that
	 * leader has a higher utility value. If not, then this node will call for a
	 * leader election
	 */
	Handler<LeaderStatusResponse> handleLeaderStatusResponse = new Handler<LeaderStatusPort.LeaderStatusResponse>() {
		@Override
		public void handle(LeaderStatusResponse event) {
			if (event.getLeader() == null
					|| (event.getLeader() != null && event.getLeader().getId() > self.getId())) {

				electionInProgress = true;
				numberOfNodesAtVotingTime = lowerNodes.size();

				// The electionCounter works as an ID for every time an election
				// is held
				// That way replies from old elections won't count
				electionCounter++;
				sendVoteRequests();
			}
		}
	};

	/**
	 * This method is called when the leader has either read enough
	 * indexMessages or when the timeout has been triggered
	 */
	private void finishIndexMsgReading() {
		// Set leadership and disallow receiving of new messages
		allowingIndexMessages = false;
		indexMessageCounter = 0;
		trigger(new LeaderStatus(iAmLeader), leaderStatusPort);

		// Cancels the timeout in case it is still going
		CancelTimeout ct = new CancelTimeout(indexMsgTimeoutId);
		trigger(ct, timerPort);
	}

	/**
	 * This class counts the votes. If the node is elected as a leader it will
	 * start collecting index messages
	 */
	private void countVotes() {
		// If all the returned votes are yes votes AND
		// there are nodes above the leader candidate in the tree AND
		// there are at least a certain number of nodes in the view AND
		// most of the voters are converged AND
		// they are above a certain ratio of the total number of nodes,
		// then the leader candidate will be elected leader

		if (yesVotes == totalVotes
				&& higherNodes.size() == 0
				&& lowerNodes.size() >= config.getMinSizeOfElectionGroup()
				&& convergedCounter >= config
						.getMinNumberOfConvergedNodes()
				&& ((float) yesVotes >= Math.ceil((float) lowerNodes.size()
						* config.getMinPercentageOfVotes()))) {

			// if you won the election while you were already a leader for some
			// reason skip the following
			if (iAmLeader == false) {
				// Create view for Snapshot
				StringBuilder builder = new StringBuilder();
				for (VodAddress node : lowerNodes) {
					builder.append(node.getId() + " ");
				}
				Snapshot.setElectionView(self.getAddress(), builder.toString());
				Snapshot.setLeaderStatus(self.getAddress(), true);

				variableCleanUp();
				iAmLeader = true;
				allowingIndexMessages = true;
				indexMessageID = UUID.nextUUID();
//				trigger(new StartIndexRequestEvent((UUID)indexMessageID), indexRoutingPort);

				// Start heart beat timeout
				SchedulePeriodicTimeout timeout = new SchedulePeriodicTimeout(
						config.getHeartbeatTimeoutDelay(),
						config.getHeartbeatTimeoutInterval());
				timeout.setTimeoutEvent(new ElectionSchedule(timeout));
				scheduledTimeoutId = timeout.getTimeoutEvent().getTimeoutId();
				trigger(timeout, timerPort);

				// Start the timeout for collecting index messages
				ScheduleTimeout indexTimeOut = new ScheduleTimeout(
						config.getIndexTimeout());
				indexTimeOut.setTimeoutEvent(new LeaderTimeout(indexTimeOut));
				indexMsgTimeoutId = indexTimeOut.getTimeoutEvent().getTimeoutId();
				trigger(indexTimeOut, timerPort);
			}
		} else {
			rejected();
		}
	}

	/**
	 * Broadcasts the vote requests to the nodes in the view
	 */
	private void sendVoteRequests() {
		ScheduleTimeout timeout = new ScheduleTimeout(config.getVoteRequestTimeout());
		timeout.setTimeoutEvent(new VoteTimeout(timeout));
		voteTimeout = timeout.getTimeoutEvent().getTimeoutId();

		ElectionMessage.Request vote;

		// Broadcasts the vote requests to the nodes in the view
		for (VodAddress receiver : lowerNodes) {
			vote = new ElectionMessage.Request(self.getAddress(), receiver, self.getId(), receiver.getId(), voteTimeout, electionCounter);
			trigger(vote, networkPort);
		}

		// trigger this after sending the messages so they all get at least the
		// appointed time
		trigger(timeout, timerPort);
	}

	/**
	 * Broadcasts the leader's current view to it's followers
	 */
	private void sendLeaderView() {

		// Broadcasts the leader's current view to it's followers
		for (VodAddress receiver : lowerNodes) {
            VotingResultMessage msg = new VotingResultMessage(self.getAddress(), receiver.getNodeAddress(), (VodAddress[])lowerNodes.toArray());
			trigger(msg, networkPort);
		}
	}

	/**
	 * Cleans up and resets the member variables
	 */
	private void variableCleanUp() {
		electionInProgress = false;

		electionCounter++;
		yesVotes = 0;
		totalVotes = 0;
		convergedCounter = 0;

        if (voteTimeout != null) {
            CancelTimeout timeout = new CancelTimeout(voteTimeout);
            trigger(timeout, timerPort);
        }

        if (scheduledTimeoutId != null) {
            CancelPeriodicTimeout periodicTimeout = new CancelPeriodicTimeout(scheduledTimeoutId);
            trigger(periodicTimeout, timerPort);
        }
	}

	/**
	 * Is called when the leader has been rejected, and makes the node into a
	 * regular node again
	 */
	private void rejected() {
		if (iAmLeader == true) {
			Snapshot.setLeaderStatus(self.getAddress(), false);
		}

		iAmLeader = false;
		trigger(new LeaderStatus(iAmLeader), leaderStatusPort);

		variableCleanUp();
	}

	private void rejected(VodAddress byNode, VodAddress betterNode) {
		rejected();
		
		// From here one could trigger an event that suggest Gradient to put this
		// better node in its view so that it won't call for more unnecessary
		// elections
		if (config.isNodeSuggestion() == true) {
			trigger(new NodeSuggestion(betterNode), leaderStatusPort);
		}
	}
}
