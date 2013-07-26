package se.sics.ms.election;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import se.sics.gvod.common.Self;
import se.sics.gvod.common.msgs.RelayMsgNetty;
import se.sics.gvod.config.ElectionConfiguration;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.timer.*;
import se.sics.kompics.*;
import se.sics.ms.timeout.IndividualTimeout;
import se.sics.peersearch.messages.*;

import se.sics.ms.gradient.BroadcastGradientPartnersPort;
import se.sics.ms.gradient.BroadcastGradientPartnersPort.GradientPartners;
import se.sics.ms.gradient.LeaderStatusPort;
import se.sics.ms.gradient.LeaderStatusPort.NodeCrashEvent;

/**
 * This class contains functions for those nodes that are directly following a
 * leader. Such functions range from leader election to how to handle the
 * scenario when no more heart beat messages are received
 */
public class ElectionFollower extends ComponentDefinition {

    Positive<Timer> timerPort = positive(Timer.class);
    Positive<VodNetwork> networkPort = positive(VodNetwork.class);
    Negative<BroadcastGradientPartnersPort> broadcast = negative(BroadcastGradientPartnersPort.class);
    Positive<LeaderStatusPort> leaderStatusPort = positive(LeaderStatusPort.class);

    private ElectionConfiguration config;
    private Self self;
    private VodAddress leader;
    private ArrayList<VodAddress> leaderView, higherNodes, lowerNodes;
    private boolean leaderIsAlive, isConverged;
    private TimeoutId heartBeatTimeoutId, deathVoteTimeout;
    private int aliveCounter, deathMessageCounter;

    /**
     * A customised timeout class used for checking when heart beat messages
     * should arrive
     */
    public class HeartBeatTimeout extends IndividualTimeout {

        public HeartBeatTimeout(ScheduleTimeout request, int id) {
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
        subscribe(handleVotingResult, networkPort);
        subscribe(handleVotingRequest, networkPort);
        subscribe(handleLeaderSuspicionRequest, networkPort);
        subscribe(handleHeartBeatTimeout, timerPort);
        subscribe(handleGradientBroadcast, broadcast);
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
            VodAddress highestNode;
            if (higherNodes.size() != 0) {
                highestNode = higherNodes.get(0);
            } else {
                highestNode = self.getAddress();
            }

            // Don't vote yes unless the source has the lowest ID
            if (highestNode.getId() < event.getSource().getId() || (leader != null && leader.getId() < event.getSource().getId())) {
                candidateAccepted = false;
            } else {
                highestNode = event.getVodSource();
            }

            ElectionMessage.Response response = new ElectionMessage.Response(self.getAddress(), event.getVodSource(), event.getTimeoutId(), event.getVoteID(), isConverged, candidateAccepted, highestNode);

            trigger(response, networkPort);
        }
    };
    /**
     * A handler receiving gradient view broadcasts, and sets its view accordingly
     */
    Handler<GradientPartners> handleGradientBroadcast = new Handler<GradientPartners>() {
        @Override
        public void handle(GradientPartners event) {
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
    Handler<LeaderViewMessage> handleVotingResult = new Handler<LeaderViewMessage>() {
        @Override
        public void handle(LeaderViewMessage event) {
            VodAddress nodeWithBestId = event.getVodSource();

            // ----------------------------------------
            // Checking which address has the lowest ID
            // ----------------------------------------

            if (leader != null) {
                if (event.getSource().getId() > leader.getId()) {
                    nodeWithBestId = leader;
                } else {
                    nodeWithBestId = event.getVodSource();
                }

            } // Only need to check nodes with higher ID than the leader
            if (higherNodes != null && higherNodes.size() != 0) {
                VodAddress highestNode = higherNodes.get(0);
                if (highestNode.getId() < nodeWithBestId.getId()) {
                    nodeWithBestId = highestNode;
                }
            }

            // ---------------
            // Choose a leader
            // ---------------

            // There is no current leader, this SHOULD not happen
            if (leader == null) {
                // The requester has the lowest ID
                if (event.getSource().getId() == nodeWithBestId.getId()) {
                    // Accept this node
                    acceptLeader(event.getVodSource(), Arrays.asList(event.getView()));
                } else {
                    // reject this node
                    rejectLeader(event.getVodSource(), nodeWithBestId);
                }
            } // The current leader has the lowest ID
            else if (event.getSource().getId() == leader.getId()
                    && leader.getId() == nodeWithBestId.getId()) {
                acceptLeader(leader, Arrays.asList(event.getView()));
            } // The current leader does NOT have the lowest ID
            else if (event.getSource().getId() == nodeWithBestId.getId()
                    && nodeWithBestId.getId() < leader.getId()) {
                rejectLeader(leader, event.getVodSource());
                acceptLeader(event.getVodSource(), Arrays.asList(event.getView()));
            } // The source is the leader, but it is not suited to lead
            else if (event.getSource().getId() == leader.getId()) {
                rejectLeader(leader, nodeWithBestId);
            } // Someone with a higher ID is sending heart beats
            else {
                rejectLeaderCandidate(event.getVodSource(), nodeWithBestId);
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
    Handler<HeartBeatTimeout> handleHeartBeatTimeout = new Handler<HeartBeatTimeout>() {
        @Override
        public void handle(HeartBeatTimeout event) {
            if (leaderIsAlive == true) {
                leaderIsAlive = false;

                RejectFollowerMessage.Request msg = new RejectFollowerMessage.Request(self.getAddress(), leader, UUID.nextUUID());
                trigger(msg, networkPort);
                scheduleHeartbeatTimeout(config.getRejectedTimeout());
            } else if (leader != null) {
                ScheduleTimeout timeout = new ScheduleTimeout(
                        config.getDeathTimeout());
                deathVoteTimeout = (UUID) timeout.getTimeoutEvent().getTimeoutId();

                for (VodAddress addr : leaderView) {
                    LeaderSuspicionMessage.Request msg = new LeaderSuspicionMessage.Request(self.getAddress(), addr, self.getId(), addr.getId(), deathVoteTimeout, leader);
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
                msg = new LeaderSuspicionMessage.Response(self.getAddress(), event.getVodSource(), event.getClientId(), event.getRemoteId(), event.getNextDest(), event.getTimeoutId(), RelayMsgNetty.Status.OK, leaderIsAlive, leader);
            } else { // Different leader
                msg = new LeaderSuspicionMessage.Response(self.getAddress(), event.getVodSource(), event.getClientId(), event.getRemoteId(), event.getNextDest(), event.getTimeoutId(), RelayMsgNetty.Status.OK, false, leader);
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
            if ((leader == null) || (event.getLeader() == null)
                    || (leader != null && leader.getId() != event.getLeader().getId())) {
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
                trigger(new NodeCrashEvent(leader), leaderStatusPort);

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
                && deathMessageCounter >= leaderView.size()
                * config.getDeathVoteMajorityPercentage()
                && aliveCounter < Math.ceil((float) leaderView.size()
                * config.getLeaderDeathMajorityPercentage())) {

            for (VodAddress addr : leaderView) {
                LeaderDeathAnnouncementMessage msg = new LeaderDeathAnnouncementMessage(self.getAddress(), addr, leader);
                trigger(msg, networkPort);
            }

            trigger(new NodeCrashEvent(leader), leaderStatusPort);
            leader = null;
            leaderView = null;
        } else { // The leader MIGHT be alive
            // TODO maybe you got rejected?
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
        cancelHeartbeatTimeout();

        ScheduleTimeout timeout = new ScheduleTimeout(delay);
        timeout.setTimeoutEvent(new HeartBeatTimeout(timeout, self.getId()));
        heartBeatTimeoutId = timeout.getTimeoutEvent().getTimeoutId();
        trigger(timeout, timerPort);
    }

    /**
     * Cancels the heart beat timeout
     */
    private void cancelHeartbeatTimeout() {
        CancelTimeout ct = new CancelTimeout(heartBeatTimeoutId);
        trigger(ct, timerPort);
    }

    /**
     * Accepts a certain leader
     *
     * @param node the leader's Address
     * @param view the leader's current view
     */
    private void acceptLeader(VodAddress node, Collection<VodAddress> view) {
        leaderIsAlive = true;
        leader = node;
        leaderView = new ArrayList<VodAddress>(view);

        // Cancel old timeouts and create a new one
        scheduleHeartbeatTimeout(config.getHeartbeatWaitTimeout());
    }

    /**
     * Rejects the leader
     *
     * @param node the leader's Addres
     * @param betterNode the better node's Address
     */
    private void rejectLeader(VodAddress node, VodAddress betterNode) {
        leader = null;
        leaderView = null;
        leaderIsAlive = false;

        // Cancel old timeouts
        cancelHeartbeatTimeout();
        RejectLeaderMessage msg = new RejectLeaderMessage(self.getAddress(), node, betterNode);
        trigger(msg, networkPort);
    }

    /**
     * Rejects a leader candidate
     *
     * @param node the leader candidate's Address
     * @param betterNode the better node's Address
     */
    private void rejectLeaderCandidate(VodAddress node, VodAddress betterNode) {
        RejectLeaderMessage msg = new RejectLeaderMessage(self.getAddress(), node, betterNode);
        trigger(msg, networkPort);
    }
}
