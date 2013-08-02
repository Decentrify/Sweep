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
import se.sics.ms.gradient.GradientViewChangePort;
import se.sics.ms.gradient.LeaderStatusPort;
import se.sics.ms.gradient.LeaderStatusPort.LeaderStatus;
import se.sics.ms.snapshot.Snapshot;
import se.sics.ms.timeout.IndividualTimeout;
import se.sics.peersearch.messages.ElectionMessage;
import se.sics.peersearch.messages.LeaderViewMessage;
import se.sics.peersearch.messages.RejectFollowerMessage;
import se.sics.peersearch.messages.RejectLeaderMessage;

import java.util.ArrayList;

/**
 * This component contains functions for how a node will find out if it is the
 * leader. It also contains functions that a leader needs in order to perform
 * leader elections etc.
 */
public class ElectionLeader extends ComponentDefinition {

	Positive<Timer> timerPort = positive(Timer.class);
	Positive<VodNetwork> networkPort = positive(VodNetwork.class);
	Negative<GradientViewChangePort> gradientViewChangePort = negative(GradientViewChangePort.class);
	Positive<LeaderStatusPort> leaderStatusPort = positive(LeaderStatusPort.class);

	private ElectionConfiguration config;
	private int numberOfNodesAtVotingTime;
	private int yesVotes, totalVotes, electionCounter, convergedCounter;
	private boolean electionInProgress, iAmLeader;
	private Self self;
	private ArrayList<VodAddress> lowerNodes, higherNodes;
	private TimeoutId heartbeatTimeoutId, voteTimeoutId;

	/**
	 * A customised timeout class for when to send heart beats etc
	 */
	public class HeartbeatSchedule extends IndividualTimeout {

		public HeartbeatSchedule(SchedulePeriodicTimeout request, int id) {
			super(request, id);
		}
	}

	public class VoteTimeout extends IndividualTimeout {

		public VoteTimeout(ScheduleTimeout request, int id) {
			super(request, id);
		}
	}

	/**
	 * Default constructor that initiates all the event subscriptions to
	 * handlers
	 */
	public ElectionLeader() {
		subscribe(handleInit, control);
		subscribe(handleHeartBeats, timerPort);
		subscribe(handleVoteTimeout, timerPort);
		subscribe(handleVotingResponse, networkPort);
		subscribe(handleLeaderRejection, networkPort);
		subscribe(handleGradientBroadcast, gradientViewChangePort);
		subscribe(handleRejectedFollower, networkPort);
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

			lowerNodes = new ArrayList<VodAddress>();
			higherNodes = new ArrayList<VodAddress>();
		}
	};

	/**
	 * Handler for the periodic Gradient views that are being sent. It checks if the
	 * node fulfills the requirements in order to become a leader, and in that
	 * case it will call for a leader election
	 */
	Handler<GradientViewChangePort.GradientViewChanged> handleGradientBroadcast = new Handler<GradientViewChangePort.GradientViewChanged>() {
		@Override
		public void handle(GradientViewChangePort.GradientViewChanged event) {
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

				startVote();
			} else if (iAmLeader && higherNodes.size() != 0) {
                rejected();
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
                    rejected();
				}
				if (event.isConvereged() == true) {
					convergedCounter++;
				}
			}

			// Count the votes if all votes have returned
			if (totalVotes >= numberOfNodesAtVotingTime) {
				evaluateVotes();
			}
		}
	};

	/**at se.sics.ms.election.ElectionLeader.evaluateVotes(ElectionLeader.java:391)
	 * A handler that will call for a vote call after a certain amount of time
	 * if not all voters have returned with a vote
	 */
	Handler<VoteTimeout> handleVoteTimeout = new Handler<VoteTimeout>() {
		@Override
		public void handle(VoteTimeout event) {
			evaluateVotes();
		}
	};

	/**
	 * A handler that will periodically send out heart beats to the node's
	 * (leader's) followers
	 */
	Handler<HeartbeatSchedule> handleHeartBeats = new Handler<HeartbeatSchedule>() {
		@Override
		public void handle(HeartbeatSchedule event) {
            sendLeaderView();
		}
	};

	/**
	 * A handler that handles rejected messages send by nodes who have found a
	 * better leader
	 */
	Handler<RejectLeaderMessage> handleLeaderRejection = new Handler<RejectLeaderMessage>() {
		@Override
		public void handle(RejectLeaderMessage event) {
            // TODO we need to check if the rejection is valid e.g. check the given better node
			rejected();
		}
	};

	/**
	 * A handler that handles nodes who have been kicked out of the leader's
	 * view and ask if the leader if still alive
	 */
	Handler<RejectFollowerMessage.Request> handleRejectedFollower = new Handler<RejectFollowerMessage.Request>() {
		@Override
		public void handle(RejectFollowerMessage.Request event) {
			boolean sourceIsInView = lowerNodes.contains(event.getVodDestination());
            RejectFollowerMessage.Response msg = new RejectFollowerMessage.Response(self.getAddress(), event.getVodSource(), UUID.nextUUID(), sourceIsInView);
			trigger(msg, networkPort);
		}
	};

	/**
	 * This class counts the votes. If the node is elected as a leader it will
	 * start collecting index messages
	 */
	private void evaluateVotes() {
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

				variableReset();
				iAmLeader = true;

				// Start heart beat timeout
				SchedulePeriodicTimeout timeout = new SchedulePeriodicTimeout(
						config.getHeartbeatTimeoutDelay(),
						config.getHeartbeatTimeoutInterval());
				timeout.setTimeoutEvent(new HeartbeatSchedule(timeout, self.getId()));
			    heartbeatTimeoutId = timeout.getTimeoutEvent().getTimeoutId();
				trigger(timeout, timerPort);

                trigger(new LeaderStatus(iAmLeader), leaderStatusPort);
			}
		} else {
			rejected();
		}
	}

	/**
	 * Broadcasts the vote requests to the nodes in the view
	 */
	private void startVote() {
        electionInProgress = true;
        numberOfNodesAtVotingTime = lowerNodes.size();
        // The electionCounter works as an ID for every time an election is held
        // That way replies from old elections won't count
        electionCounter++;

		ScheduleTimeout timeout = new ScheduleTimeout(config.getVoteRequestTimeout());
		timeout.setTimeoutEvent(new VoteTimeout(timeout, self.getId()));
		voteTimeoutId = timeout.getTimeoutEvent().getTimeoutId();

		ElectionMessage.Request vote;

		// Broadcasts the vote requests to the nodes in the view
		for (VodAddress receiver : lowerNodes) {
			vote = new ElectionMessage.Request(self.getAddress(), receiver, voteTimeoutId, electionCounter);
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
            // TODO don't send the view every time
            LeaderViewMessage msg = new LeaderViewMessage(self.getAddress(), receiver, lowerNodes.toArray(new VodAddress[lowerNodes.size()]));
			trigger(msg, networkPort);
		}
	}

	/**
	 * Cleans up and resets the member variables
	 */
	private void variableReset() {
		electionInProgress = false;

		yesVotes = 0;
		totalVotes = 0;
		convergedCounter = 0;

        if (voteTimeoutId != null) {
            CancelTimeout timeout = new CancelTimeout(voteTimeoutId);
            trigger(timeout, timerPort);
        }

        if (heartbeatTimeoutId != null) {
            CancelPeriodicTimeout periodicTimeout = new CancelPeriodicTimeout(heartbeatTimeoutId);
            trigger(periodicTimeout, timerPort);
        }
	}

	/**
	 * Is called when the leader has been rejected, and makes the node into a
	 * regular node again
	 */
	private void rejected() {
        if (!electionInProgress && !iAmLeader) {
            return;
        }

		if (iAmLeader == true) {
            iAmLeader = false;
            trigger(new LeaderStatus(iAmLeader), leaderStatusPort);
            Snapshot.setLeaderStatus(self.getAddress(), false);
		}
		variableReset();
	}
}
