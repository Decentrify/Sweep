package se.sics.ms.election;

import se.sics.gvod.common.Self;
import se.sics.gvod.common.VodDescriptor;
import se.sics.gvod.common.msgs.RelayMsgNetty;
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
import se.sics.ms.gradient.LeaderStatusPort.NodeCrashEvent;
import se.sics.ms.gradient.UtilityComparator;
import se.sics.ms.timeout.IndividualTimeout;
import se.sics.peersearch.messages.*;

import java.util.Set;
import java.util.SortedSet;

/**
 * This class contains functions for those nodes that are directly following a
 * leader. Such functions range from leader election to how to handle the
 * scenario when no more heart beat messages are received
 */
public class ElectionFollower extends ComponentDefinition {

    Positive<Timer> timerPort = positive(Timer.class);
    Positive<VodNetwork> networkPort = positive(VodNetwork.class);
    Negative<GradientViewChangePort> gradientViewChangePort = negative(GradientViewChangePort.class);
    Positive<LeaderStatusPort> leaderStatusPort = positive(LeaderStatusPort.class);

    private ElectionConfiguration config;
    private Self self;
    private VodDescriptor leader;
    private SortedSet<VodDescriptor> higherUtilityNodes;
    private Set<VodDescriptor> leaderView;
    private boolean leaderIsAlive, isConverged;
    private TimeoutId heartBeatTimeoutId, deathVoteTimeout;
    private int aliveCounter, deathMessageCounter;
    private UtilityComparator utilityComparator = new UtilityComparator();

    /**
     * A customised timeout class used for checking when heart beat messages
     * should arrive
     */
    public class HeartbeatTimeout extends IndividualTimeout {

        public HeartbeatTimeout(ScheduleTimeout request, int id) {
            super(request, id);
        }
    }

    /**
     * A customised timeout class used to determine how long a node should wait
     * for other nodes to reply to leader death message
     */
    public class DeathTimeout extends IndividualTimeout {

        public DeathTimeout(ScheduleTimeout request, int id) {
            super(request, id);
        }
    }

    /**
     * Default constructor that subscribes certain handlers to ports
     */
    public ElectionFollower() {
        subscribe(handleInit, control);
        subscribe(handleDeathTimeout, timerPort);
        subscribe(handleLeaderDeathAnnouncement, networkPort);
        subscribe(handleHeartbeat, networkPort);
        subscribe(handleVotingRequest, networkPort);
        subscribe(handleLeaderSuspicionRequest, networkPort);
        subscribe(handleHeartbeatTimeout, timerPort);
        subscribe(handleGradientBroadcast, gradientViewChangePort);
        subscribe(handleLeaderSuspicionResponse, networkPort);
        subscribe(handleRejectionConfirmation, networkPort);
    }
    /**
     * The initialisation handler. Called when the component is created and
     * mainly initiates variables
     */
    Handler<ElectionInit> handleInit = new Handler<ElectionInit>() {
        @Override
        public void handle(ElectionInit init) {
            self = init.getSelf();
            config = init.getConfig();
        }
    };
    /**
     * A handler that will respond to voting requests sent from leader
     * candidates. It checks if that leader candidate is a suitable leader
     */
    Handler<ElectionMessage.Request> handleVotingRequest = new Handler<ElectionMessage.Request>() {
        @Override
        public void handle(ElectionMessage.Request event) {
            boolean candidateAccepted = true;
            VodDescriptor highestUtilityNode = getHighestUtilityNode();

            // Don't vote yes unless the source has the highest utility
            if (utilityComparator.compare(highestUtilityNode, event.getLeaderCandidateDescriptor()) == 1) {
                candidateAccepted = false;
            }

            ElectionMessage.Response response = new ElectionMessage.Response(
                    self.getAddress(),
                    event.getVodSource(),
                    event.getTimeoutId(),
                    event.getVoteID(),
                    isConverged,
                    candidateAccepted,
                    highestUtilityNode);

            trigger(response, networkPort);
        }
    };

    private VodDescriptor getHighestUtilityNode() {
        VodDescriptor vodDescriptor;
        if (higherUtilityNodes.size() != 0) {
            vodDescriptor = higherUtilityNodes.last();
        } else {
            vodDescriptor = self.getDescriptor();
        }

        return  vodDescriptor;
    }

    private VodDescriptor getHighestUtilityNode(VodDescriptor vodDescriptor) {
        if (higherUtilityNodes.size() != 0) {
            vodDescriptor = utilityComparator.compare(higherUtilityNodes.last(), vodDescriptor) == 1 ? higherUtilityNodes.last() : vodDescriptor;
        } else {
            vodDescriptor = self.getDescriptor();
        }

        return  vodDescriptor;
    }

    /**
     * A handler receiving gradient view broadcasts, and sets its view accordingly
     */
    Handler<GradientViewChangePort.GradientViewChanged> handleGradientBroadcast = new Handler<GradientViewChangePort.GradientViewChanged>() {
        @Override
        public void handle(GradientViewChangePort.GradientViewChanged event) {
            isConverged = event.isConverged();
            higherUtilityNodes = event.getHigherUtilityNodes(self.getDescriptor());
        }
    };
    /**
     * Handler for receiving of heart beat messages from the leader. It checks
     * if that node is still a suitable leader and handles the situation
     * accordingly
     */
    Handler<LeaderViewMessage> handleHeartbeat = new Handler<LeaderViewMessage>() {
        @Override
        public void handle(LeaderViewMessage event) {
            VodDescriptor highestUtilityNode = getHighestUtilityNode(event.getLeaderVodDescriptor());

            if (leader == null) {
                if (event.getLeaderVodDescriptor().equals(highestUtilityNode)) {
                    acceptLeader(event.getLeaderVodDescriptor(), event.getVodDescriptors());
                } else {
                    rejectLeader(event.getVodSource(), highestUtilityNode);
                }
            } else if (event.getLeaderVodDescriptor().equals(highestUtilityNode) == false) {
                rejectLeader(event.getVodSource(), highestUtilityNode);
            } else if (event.getSource().equals(leader.getVodAddress())) {
                acceptLeader(leader, event.getVodDescriptors());
            } else {
                rejectLeader(leader.getVodAddress(), highestUtilityNode);
                acceptLeader(event.getLeaderVodDescriptor(), event.getVodDescriptors());
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
    Handler<HeartbeatTimeout> handleHeartbeatTimeout = new Handler<HeartbeatTimeout>() {
        @Override
        public void handle(HeartbeatTimeout event) {
            if (leaderIsAlive == true) {
                leaderIsAlive = false;

                RejectFollowerMessage.Request msg = new RejectFollowerMessage.Request(self.getAddress(), leader.getVodAddress(), UUID.nextUUID());
                trigger(msg, networkPort);
                scheduleHeartbeatTimeout(config.getRejectedTimeout());
            } else if (leader != null) {
                ScheduleTimeout timeout = new ScheduleTimeout(config.getDeathTimeout());
                deathVoteTimeout = timeout.getTimeoutEvent().getTimeoutId();

                for (VodDescriptor addr : leaderView) {
                    LeaderSuspicionMessage.Request msg = new LeaderSuspicionMessage.Request(
                            self.getAddress(),
                            addr.getVodAddress(),
                            self.getId(),
                            addr.getVodAddress().getId(),
                            deathVoteTimeout,
                            leader.getVodAddress());
                    trigger(msg, networkPort);
                }

                timeout.setTimeoutEvent(new DeathTimeout(timeout, self.getId()));
            }
        }
    };
    /**
     * A handler that receives rejected confirmation messages from the leader.
     * The node will reject the leader in case it has been kicked from the
     * leader's view, and is therefore no longer in the voting group
     */
    Handler<RejectFollowerMessage.Response> handleRejectionConfirmation = new Handler<RejectFollowerMessage.Response>() {
        @Override
        public void handle(RejectFollowerMessage.Response event) {
            if (event.isNodeInView() == true) {
                leaderIsAlive = true;
            } else {
                leader = null;
                leaderView = null;
                leaderIsAlive = false;

                cancelHeartbeatTimeout();
            }
        }
    };
    /**
     * A handler that will respond whether it thinks that the leader is dead or
     * not
     */
    Handler<LeaderSuspicionMessage.Request> handleLeaderSuspicionRequest = new Handler<LeaderSuspicionMessage.Request>() {
        @Override
        public void handle(LeaderSuspicionMessage.Request event) {
            LeaderSuspicionMessage.Response msg;

            // Same leader
            if (leader != null && event.getLeader().getId() == leader.getId()) {
                msg = new LeaderSuspicionMessage.Response(self.getAddress(), event.getVodSource(), event.getClientId(), event.getRemoteId(), event.getNextDest(), event.getTimeoutId(), RelayMsgNetty.Status.OK, leaderIsAlive, leader.getVodAddress());
            } else { // Different leader
                msg = new LeaderSuspicionMessage.Response(self.getAddress(), event.getVodSource(), event.getClientId(), event.getRemoteId(), event.getNextDest(), event.getTimeoutId(), RelayMsgNetty.Status.OK, false, leader.getVodAddress());
            }

            trigger(msg, networkPort);
        }
    };
    /**
     * A handler that counts how many death responses have been received. If the
     * number of votes are the same as the number of nodes in the leader's view,
     * then it calls upon a vote count
     */
    Handler<LeaderSuspicionMessage.Response> handleLeaderSuspicionResponse = new Handler<LeaderSuspicionMessage.Response>() {
        @Override
        public void handle(LeaderSuspicionMessage.Response event) {
            // TODO Somebody could send fake responses here and make everybody think the leader is dead
            if ((leader == null) || (event.getLeader() == null) || (leader != null && leader.getId() != event.getLeader().getId())) {
                return;
            }

            deathMessageCounter++;
            if (event.isSuspected() == true) {
                aliveCounter++;
            }

            if (leaderView != null && deathMessageCounter == leaderView.size()) {
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
    Handler<LeaderDeathAnnouncementMessage> handleLeaderDeathAnnouncement = new Handler<LeaderDeathAnnouncementMessage>() {
        @Override
        public void handle(LeaderDeathAnnouncementMessage event) {
            if (leader != null && event.getLeader().getId() == leader.getId()) {
                // cancel timeout and reset
                cancelHeartbeatTimeout();
                trigger(new NodeCrashEvent(leader.getVodAddress()), leaderStatusPort);

                leader = null;
                leaderView = null;
                leaderIsAlive = false;
            }
        }
    };

    /**
     * Counts the votes from its leader death election
     */
    private void evaluateDeathResponses() {

        if (deathVoteTimeout != null) {
            CancelTimeout timeout = new CancelTimeout(deathVoteTimeout);
            trigger(timeout, timerPort);
        }

        // The leader is considered dead
        if (leader != null
                && leaderView != null
                && deathMessageCounter >= leaderView.size() * config.getDeathVoteMajorityPercentage()
                && aliveCounter < Math.ceil((float) leaderView.size() * config.getLeaderDeathMajorityPercentage())) {

            for (VodDescriptor vodDescriptor : leaderView) {
                LeaderDeathAnnouncementMessage msg = new LeaderDeathAnnouncementMessage(self.getAddress(), vodDescriptor.getVodAddress(), leader);
                trigger(msg, networkPort);
            }

            trigger(new NodeCrashEvent(leader.getVodAddress()), leaderStatusPort);
            leader = null;
            leaderView = null;
        } else { // The leader MIGHT be alive
            leaderIsAlive = true;
            scheduleHeartbeatTimeout(config.getHeartbeatWaitTimeout());
        }

        deathMessageCounter = 0;
        aliveCounter = 0;
    }

    /**
     * Starts the next heart beat timeout using a custom period
     *
     * @param delay
     * custom period in ms
     */
    private void scheduleHeartbeatTimeout(int delay) {
        ScheduleTimeout timeout = new ScheduleTimeout(delay);
        timeout.setTimeoutEvent(new HeartbeatTimeout(timeout, self.getId()));
        heartBeatTimeoutId = timeout.getTimeoutEvent().getTimeoutId();
        trigger(timeout, timerPort);
    }

    /**
     * Cancels the heart beat timeout
     */
    private void cancelHeartbeatTimeout() {
        if (heartBeatTimeoutId != null) {
            CancelTimeout ct = new CancelTimeout(heartBeatTimeoutId);
            trigger(ct, timerPort);
        }
        heartBeatTimeoutId = null;
    }

    /**
     * Accepts a certain leader
     *
     * @param node the leader's Address
     * @param view the leader's current view
     */
    private void acceptLeader(VodDescriptor node, Set<VodDescriptor> view) {
        leaderIsAlive = true;
        leader = node;
        leaderView = view;

        // Cancel old timeouts and create a new one
        scheduleHeartbeatTimeout(config.getHeartbeatWaitTimeout());
    }

    /**
     * Rejects the leader
     *
     * @param node the leader's address
     * @param betterNode the better node's descriptor
     */
    private void rejectLeader(VodAddress node, VodDescriptor betterNode) {
        leader = null;
        leaderView = null;
        leaderIsAlive = false;

        // Cancel old timeouts
        cancelHeartbeatTimeout();
        RejectLeaderMessage msg = new RejectLeaderMessage(self.getAddress(), node, betterNode);
        trigger(msg, networkPort);
    }
}
