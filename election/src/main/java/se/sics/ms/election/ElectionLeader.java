package se.sics.ms.election;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.common.Self;
import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.config.ElectionConfiguration;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.timer.*;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.ms.gradient.GradientViewChangePort;
import se.sics.ms.gradient.LeaderStatusPort;
import se.sics.ms.gradient.LeaderStatusPort.LeaderStatus;
import se.sics.ms.gradient.UtilityComparator;
import se.sics.ms.messages.ElectionMessage;
import se.sics.ms.messages.LeaderViewMessage;
import se.sics.ms.messages.RejectFollowerMessage;
import se.sics.ms.messages.RejectLeaderMessage;
import se.sics.ms.snapshot.Snapshot;
import se.sics.ms.timeout.IndividualTimeout;

import java.util.SortedSet;

/**
 * This component contains functions for how a node will find out if it is the
 * leader. It also contains functions that a leader needs in order to perform
 * leader elections etc.
 */
public class ElectionLeader extends ComponentDefinition {

    private static final Logger logger = LoggerFactory.getLogger(ElectionLeader.class);

	Positive<Timer> timerPort = positive(Timer.class);
	Positive<VodNetwork> networkPort = positive(VodNetwork.class);
	Negative<GradientViewChangePort> gradientViewChangePort = negative(GradientViewChangePort.class);
	Positive<LeaderStatusPort> leaderStatusPort = positive(LeaderStatusPort.class);

	private ElectionConfiguration config;
	private int numberOfNodesAtVotingTime;
	private int yesVotes, totalVotes, electionCounter, convergedNodesCounter;
	private boolean electionInProgress, iAmLeader;
	private Self self;
	private SortedSet<VodDescriptor> lowerUtilityNodes, higherUtilityNodes;
	private TimeoutId heartbeatTimeoutId, voteTimeoutId;
    private final UtilityComparator utilityComparator = new UtilityComparator();

	/**
	 * QueryLimit customised timeout class for when to send heart beats etc
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
		subscribe(handleHeartbeats, timerPort);
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
			higherUtilityNodes = event.getHigherUtilityNodes(self.getDescriptor());
			lowerUtilityNodes = event.getLowerUtilityNodes(self.getDescriptor());

			// Create view for Snapshot
			StringBuilder builder = new StringBuilder();
			for (VodDescriptor node : higherUtilityNodes) {
				builder.append(node.getVodAddress().getId() + " ");
			}
			for (VodDescriptor node : lowerUtilityNodes) {
				builder.append(node.getVodAddress().getId() + " ");
			}
			Snapshot.setCurrentView(self.getAddress(), builder.toString());

			if (event.isConverged()
					&& !iAmLeader
					&& !electionInProgress
					&& higherUtilityNodes.size() == 0
					&& lowerUtilityNodes.size() >= config.getMinSizeOfElectionGroup()) {

				startVote();
			} else if (iAmLeader && higherUtilityNodes.size() != 0) {
                rejected();
            }
		}
	};

	/**
	 * QueryLimit handler that counts the number of votes received from the followers. If
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
				}
				if (event.isConvereged() == true) {
					convergedNodesCounter++;
				}
			}

			// Count the votes if all votes have returned
			if (totalVotes >= numberOfNodesAtVotingTime) {
				evaluateVotes();
			}
		}
	};

	/**
	 * QueryLimit handler that will call for a vote call after a certain amount of time
	 * if not all voters have returned with a vote
	 */
	Handler<VoteTimeout> handleVoteTimeout = new Handler<VoteTimeout>() {
		@Override
		public void handle(VoteTimeout event) {
			evaluateVotes();
		}
	};

	/**
	 * QueryLimit handler that will periodically send out heart beats to the node's
	 * (leader's) followers
	 */
	Handler<HeartbeatSchedule> handleHeartbeats = new Handler<HeartbeatSchedule>() {
		@Override
		public void handle(HeartbeatSchedule event) {
            sendLeaderView();
		}
	};

	/**
	 * QueryLimit handler that handles rejected messages send by nodes who have found a
	 * better leader
	 */
	Handler<RejectLeaderMessage> handleLeaderRejection = new Handler<RejectLeaderMessage>() {
		@Override
		public void handle(RejectLeaderMessage event) {
            // TODO we need to check if the rejection is valid e.g. check the given better node
            if (utilityComparator.compare(self.getDescriptor(), event.getBetterLeader()) == 1) {
                return;
            }
			rejected();
		}
	};

	/**
	 * QueryLimit handler that handles nodes who have been kicked out of the leader's
	 * view and ask if the leader if still alive
	 */
	Handler<RejectFollowerMessage.Request> handleRejectedFollower = new Handler<RejectFollowerMessage.Request>() {
		@Override
		public void handle(RejectFollowerMessage.Request event) {
			boolean sourceIsInView = false;

            for (VodDescriptor vodDescriptor : lowerUtilityNodes) {
                if (vodDescriptor.getVodAddress().equals(event.getVodSource())) {
                    sourceIsInView = true;
                    break;
                }
            }

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
				&& higherUtilityNodes.size() == 0
				&& lowerUtilityNodes.size() >= config.getMinSizeOfElectionGroup()
				&& convergedNodesCounter >= config.getMinNumberOfConvergedNodes()
				&& ((float) yesVotes >= Math.ceil((float) lowerUtilityNodes.size() * config.getMinPercentageOfVotes()))) {

			// if you won the election while you were already a leader for some
			// reason skip the following
			if (iAmLeader == false) {
				// Create view for Snapshot
				StringBuilder builder = new StringBuilder();
				for (VodDescriptor node : lowerUtilityNodes) {
					builder.append(node.getVodAddress().getId() + " ");
				}
				Snapshot.setElectionView(self.getAddress(), builder.toString());
				Snapshot.setLeaderStatus(self.getAddress(), true);

				variableReset();
				iAmLeader = true;

				// Start heart beat timeout
				SchedulePeriodicTimeout timeout = new SchedulePeriodicTimeout(config.getHeartbeatTimeoutDelay(), config.getHeartbeatTimeoutInterval());
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
        numberOfNodesAtVotingTime = lowerUtilityNodes.size();
        // The electionCounter works as an ID for every time an election is held
        // That way replies from old elections won't count
        electionCounter++;

		ScheduleTimeout timeout = new ScheduleTimeout(config.getVoteRequestTimeout());
		timeout.setTimeoutEvent(new VoteTimeout(timeout, self.getId()));
		voteTimeoutId = timeout.getTimeoutEvent().getTimeoutId();

		ElectionMessage.Request vote;

		// Broadcasts the vote requests to the nodes in the view
		for (VodDescriptor receiver : lowerUtilityNodes) {
			vote = new ElectionMessage.Request(self.getAddress(), receiver.getVodAddress(), voteTimeoutId, electionCounter, self.getDescriptor());
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
		for (VodDescriptor receiver : lowerUtilityNodes) {
            // TODO don't send the view every time
            LeaderViewMessage msg = new LeaderViewMessage(self.getAddress(), receiver.getVodAddress(), self.getDescriptor(), lowerUtilityNodes);
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
		convergedNodesCounter = 0;

        if (voteTimeoutId != null) {
            CancelTimeout timeout = new CancelTimeout(voteTimeoutId);
            trigger(timeout, timerPort);
        }

        if (heartbeatTimeoutId != null) {
            CancelPeriodicTimeout periodicTimeout = new CancelPeriodicTimeout(heartbeatTimeoutId);
            trigger(periodicTimeout, timerPort);
        }

        voteTimeoutId = null;
        heartbeatTimeoutId = null;
	}

	/**
	 * Is called when the leader has been rejected, and makes the node into a
	 * regular node again
	 */
	private void rejected() {
        iAmLeader = false;
        trigger(new LeaderStatus(iAmLeader), leaderStatusPort);
        Snapshot.setLeaderStatus(self.getAddress(), false);
		variableReset();
	}
}
