//package se.sics.ms.election;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import se.sics.co.FailureDetectorPort;
//import se.sics.gvod.common.Self;
//import se.sics.gvod.common.msgs.RelayMsgNetty;
//import se.sics.gvod.config.ElectionConfiguration;
//import se.sics.gvod.net.VodAddress;
//import se.sics.gvod.net.VodNetwork;
//import se.sics.gvod.timer.*;
//import se.sics.gvod.timer.Timer;
//import se.sics.gvod.timer.UUID;
//import se.sics.kompics.ComponentDefinition;
//import se.sics.kompics.Handler;
//import se.sics.kompics.Negative;
//import se.sics.kompics.Positive;
//import se.sics.ms.common.MsSelfImpl;
//import se.sics.ms.gradient.events.LeaderInfoUpdate;
//import se.sics.ms.gradient.misc.UtilityComparator;
//import se.sics.ms.gradient.ports.GradientViewChangePort;
//import se.sics.ms.gradient.ports.LeaderStatusPort;
//import se.sics.ms.gradient.ports.PublicKeyPort;
//import se.sics.ms.messages.*;
//import se.sics.ms.serializer.SearchDescriptorSerializer;
//import se.sics.ms.timeout.IndividualTimeout;
//import se.sics.ms.types.PartitionId;
//import se.sics.ms.types.SearchDescriptor;
//
//import java.security.PublicKey;
//import java.util.*;
//
//
///**
// * This class contains functions for those nodes that are directly following a
// * leader. Such functions range from leader election to how to handle the
// * scenario when no more heart beat messages are received
// */
//public class ElectionFollower extends ComponentDefinition {
//
//    private static final Logger logger = LoggerFactory.getLogger(ElectionFollower.class);
//
//    Positive<Timer> timerPort = positive(Timer.class);
//    Positive<VodNetwork> networkPort = positive(VodNetwork.class);
//    Positive<FailureDetectorPort> fdPort = requires(FailureDetectorPort.class);
//    Positive<LeaderStatusPort> leaderStatusPort = positive(LeaderStatusPort.class);
//    Positive<PublicKeyPort> publicKeyPort = positive(PublicKeyPort.class);
//    Negative<GradientViewChangePort> gradientViewChangePort = negative(GradientViewChangePort.class);
//
//    private ElectionConfiguration config;
//    private MsSelfImpl self;
//    private SearchDescriptor leader;
//    private SortedSet<SearchDescriptor> higherUtilityNodes;
//    private Set<SearchDescriptor> leaderView;
//    private boolean leaderIsAlive, isConverged;
//    private TimeoutId heartBeatTimeoutId, deathVoteTimeout;
//    private int aliveCounter, deathMessageCounter;
//    private UtilityComparator utilityComparator = new UtilityComparator();
//
//    /**
//     * QueryLimit customised timeout class used for checking when heart beat messages
//     * should arrive
//     */
//    public class HeartbeatTimeout extends IndividualTimeout {
//
//        public HeartbeatTimeout(ScheduleTimeout request, int id) {
//            super(request, id);
//        }
//    }
//
//    /**
//     * QueryLimit customised timeout class used to determine how long a node should wait
//     * for other nodes to reply to leader death message
//     */
//    public class DeathTimeout extends IndividualTimeout {
//
//        public DeathTimeout(ScheduleTimeout request, int id) {
//            super(request, id);
//        }
//    }
//
//    /**
//     * Default constructor that subscribes certain handlers to ports
//     */
//    public ElectionFollower(ElectionInit<ElectionFollower> init) {
//        doInit(init);
//        subscribe(handleDeathTimeout, timerPort);
////        subscribe(handleLeaderDeathAnnouncement, networkPort);
//        subscribe(handleHeartbeat, networkPort);
//        subscribe(handleVotingRequest, networkPort);
//        subscribe(handleLeaderSuspicionRequest, networkPort);
//        subscribe(handleHeartbeatTimeout, timerPort);
//        subscribe(handleGradientBroadcast, gradientViewChangePort);
//        subscribe(handleLeaderSuspicionResponse, networkPort);
//        subscribe(handleRejectionConfirmation, networkPort);
////        subscribe(handleTerminateBeingLeader, leaderStatusPort);
//        subscribe(handleFailureDetector, fdPort);
//    }
//    /**
//     * The initialisation handler. Called when the component is created and
//     * mainly initiates variables
//     */
//    public void doInit(ElectionInit init) {
//        self = (MsSelfImpl)init.getSelf();
//        config = init.getConfig();
//        higherUtilityNodes = new TreeSet<SearchDescriptor>();
//    }
//
//    /**
//     * QueryLimit handler that will respond to voting requests sent from leader
//     * candidates. It checks if that leader candidate is a suitable leader
//     */
//    final Handler<ElectionMessage.Request> handleVotingRequest = new Handler<ElectionMessage.Request>() {
//        @Override
//        public void handle(ElectionMessage.Request event) {
//            boolean candidateAccepted = true;
//            SearchDescriptor highestUtilityNode = getHighestUtilityNode();
//
//            // Don't vote yes unless the source has the highest utility
//            if (utilityComparator.compare(highestUtilityNode, event.getLeaderCandidateDescriptor()) == 1) {
//                candidateAccepted = false;
//            }
//
//            ElectionMessage.Response response = new ElectionMessage.Response(
//                    self.getAddress(),
//                    event.getVodSource(),
//                    event.getTimeoutId(),
//                    event.getVoteID(),
//                    isConverged,
//                    candidateAccepted,
//                    highestUtilityNode);
//
//            trigger(response, networkPort);
//        }
//    };
//
//
//    /**
//     * Search stored node set and see we can find any higher utility node.
//     * Else return the self descriptor.
//     *
//     * @return Highest Utility Node.
//     */
//    private SearchDescriptor getHighestUtilityNode() {
//        //removeNodesFromOtherPartitions();
//        SearchDescriptor searchDescriptor;
//        if (higherUtilityNodes.size() != 0) {
//            searchDescriptor = higherUtilityNodes.last();
//        } else {
//            searchDescriptor = self.getSelfDescriptor();
//        }
//
//        return  searchDescriptor;
//    }
//
//
//    /**
//     * Iterate over the sample set and compare the utility with the supplied value.
//     *
//     * @param searchDescriptor Descriptor to check utility again.
//     * @return Descriptor with Biggest Utility.
//     */
//    private SearchDescriptor getHighestUtilityNode(SearchDescriptor searchDescriptor) {
//        //removeNodesFromOtherPartitions();
//        if (higherUtilityNodes.size() != 0) {
//            searchDescriptor = utilityComparator.compare(higherUtilityNodes.last(), searchDescriptor) == 1 ? higherUtilityNodes.last() : searchDescriptor;
//        } else {
//            searchDescriptor = self.getSelfDescriptor();
//        }
//
//        return  searchDescriptor;
//    }
//
//    /**
//     * QueryLimit handler receiving gradient view broadcasts, and sets its view accordingly
//     */
//    final Handler<GradientViewChangePort.GradientViewChanged> handleGradientBroadcast = new Handler<GradientViewChangePort.GradientViewChanged>() {
//        @Override
//        public void handle(GradientViewChangePort.GradientViewChanged event) {
//            isConverged = event.isConverged();
//
//            // WARNING: Always create copy of set and then calculate tail set.
//            // If you directly try to add tail set to constructor, sometimes weird exceptions come.
//
//            SortedSet<SearchDescriptor> gradientSet = new TreeSet<SearchDescriptor>(event.getGradientView());
//            higherUtilityNodes = gradientSet.tailSet(self.getSelfDescriptor());
//        }
//    };
//    /**
//     * Handler for receiving of heart beat messages from the leader. It checks
//     * if that node is still a suitable leader and handles the situation
//     * accordingly
//     */
//    final Handler<LeaderViewMessage> handleHeartbeat = new Handler<LeaderViewMessage>() {
//        @Override
//        public void handle(LeaderViewMessage event) {
//            SearchDescriptor highestUtilityNode = getHighestUtilityNode(event.getLeaderSearchDescriptor());
//
//            if(highestUtilityNode.getOverlayAddress().getPartitionId() != self.getPartitionId()
//                    || highestUtilityNode.getOverlayAddress().getPartitionIdDepth() != self.getPartitionIdDepth()
//                    || highestUtilityNode.getOverlayAddress().getPartitioningType() != self.getPartitioningType())
//                highestUtilityNode = event.getLeaderSearchDescriptor();
//
//            if (leader == null) {
//                if (event.getLeaderSearchDescriptor().equals(highestUtilityNode)) {
//                    acceptLeader(event.getLeaderSearchDescriptor(), event.getSearchDescriptors(), event.getLeaderPublicKey());
//                } else {
//                    rejectLeader(event.getVodSource(), highestUtilityNode);
//                }
//            } else if (!event.getLeaderSearchDescriptor().equals(highestUtilityNode)) {
//                rejectLeader(event.getVodSource(), highestUtilityNode);
//            } else if (event.getLeaderSearchDescriptor().equals(leader)) {
//                acceptLeader(leader, event.getSearchDescriptors(), event.getLeaderPublicKey());
//            } else {
//                rejectLeader(leader.getVodAddress(), highestUtilityNode);
//                acceptLeader(event.getLeaderSearchDescriptor(), event.getSearchDescriptors(), event.getLeaderPublicKey());
//            }
//        }
//    };
//
//    /**
//     * QueryLimit handler responsible for the actions taken when the node has not
//     * received a heart beat message from the leader for a certain amount of
//     * time. First it will try to ask the leader if it has been kicked out of
//     * the leader's view. If there is no response from the leader it will call
//     * for a leader death vote. It won't call for a leader death vote right away
//     * in case the local version of the leaver's view is completely outdated,
//     * because that could result in valid leaders being rejected.
//     */
//    final Handler<HeartbeatTimeout> handleHeartbeatTimeout = new Handler<HeartbeatTimeout>() {
//        @Override
//        public void handle(HeartbeatTimeout event) {
//            if (leaderIsAlive) {
//
//                leaderIsAlive = false;
//
//                RejectFollowerMessage.Request msg = new RejectFollowerMessage.Request(self.getAddress(), leader.getVodAddress(), UUID.nextUUID());
//                trigger(msg, networkPort);
//                scheduleHeartbeatTimeout(config.getRejectedTimeout());
//            } else if (leader != null) {
//                ScheduleTimeout timeout = new ScheduleTimeout(config.getDeathTimeout());
//                timeout.setTimeoutEvent(new DeathTimeout(timeout, self.getId()));
//                deathVoteTimeout = timeout.getTimeoutEvent().getTimeoutId();
//
//                for (SearchDescriptor addr : leaderView) {
//                    LeaderSuspicionMessage.Request msg = new LeaderSuspicionMessage.Request(
//                            self.getAddress(),
//                            addr.getVodAddress(),
//                            self.getId(),
//                            addr.getVodAddress().getId(),
//                            deathVoteTimeout,
//                            leader.getVodAddress());
//                    trigger(msg, networkPort);
//                }
//            }
//        }
//    };
//
//
//    /**
//     * QueryLimit handler that receives rejected confirmation messages from the leader.
//     * The node will reject the leader in case it has been kicked from the
//     * leader's view, and is therefore no longer in the voting group
//     */
//    final Handler<RejectFollowerMessage.Response> handleRejectionConfirmation = new Handler<RejectFollowerMessage.Response>() {
//        @Override
//        public void handle(RejectFollowerMessage.Response event) {
//            if (event.isNodeInView()) {
//                leaderIsAlive = true;
//            } else {
//                resetLeader();
//
//                cancelHeartbeatTimeout();
//            }
//        }
//    };
//
//    /**
//     * QueryLimit handler that will respond whether it thinks that the leader is dead or
//     * not
//     */
//    final Handler<LeaderSuspicionMessage.Request> handleLeaderSuspicionRequest = new Handler<LeaderSuspicionMessage.Request>() {
//        @Override
//        public void handle(LeaderSuspicionMessage.Request event) {
//            LeaderSuspicionMessage.Response msg;
//
//            // Same leader
//            if (leader != null && event.getLeader().getId() == leader.getId()) {
//                msg = new LeaderSuspicionMessage.Response(self.getAddress(), event.getVodSource(), event.getClientId(), event.getRemoteId(), event.getNextDest(), event.getTimeoutId(), RelayMsgNetty.Status.OK, leaderIsAlive, leader.getVodAddress());
//            } else { // Different leader
//                msg = new LeaderSuspicionMessage.Response(self.getAddress(), event.getVodSource(), event.getClientId(), event.getRemoteId(), event.getNextDest(), event.getTimeoutId(), RelayMsgNetty.Status.OK, false, leader.getVodAddress());
//            }
//
//            trigger(msg, networkPort);
//        }
//    };
//
//    /**
//     * QueryLimit handler that counts how many death responses have been received. If the
//     * number of votes are the same as the number of nodes in the leader's view,
//     * then it calls upon a vote count
//     */
//    final Handler<LeaderSuspicionMessage.Response> handleLeaderSuspicionResponse = new Handler<LeaderSuspicionMessage.Response>() {
//        @Override
//        public void handle(LeaderSuspicionMessage.Response event) {
//            // TODO Somebody could send fake responses here and make everybody think the leader is dead
//            if ((leader == null) || (event.getLeader() == null) || (leader != null && leader.getId() != event.getLeader().getId())) {
//                return;
//            }
//
//            deathMessageCounter++;
//            if (event.isSuspected()) {
//                aliveCounter++;
//            }
//
//            if (leaderView != null && deathMessageCounter == leaderView.size()) {
//                evaluateDeathResponses();
//            }
//        }
//    };
//
//    /**
//     * QueryLimit handler that listens for DeathTimeout event and will then call for an
//     * evaluation of death responses
//     */
//    final Handler<DeathTimeout> handleDeathTimeout = new Handler<ElectionFollower.DeathTimeout>() {
//        @Override
//        public void handle(DeathTimeout event) {
//            evaluateDeathResponses();
//        }
//    };
//
////    /**
////     * QueryLimit handler that will set the leader to null in case the other nodes have
////     * confirmed the leader to be dead
////     */
////    final Handler<LeaderDeathAnnouncementMessage> handleLeaderDeathAnnouncement = new Handler<LeaderDeathAnnouncementMessage>() {
////        @Override
////        public void handle(LeaderDeathAnnouncementMessage event) {
////            if (leader != null && event.getLeader().getId() == leader.getId()) {
////                // cancel timeout and reset
////                cancelHeartbeatTimeout();
////                trigger(new NodeCrashEvent(leader.getVodAddress()), leaderStatusPort);
////
////                resetLeader();
////            }
////        }
////    };
//
////    final Handler<LeaderStatusPort.TerminateBeingLeader> handleTerminateBeingLeader = new Handler<LeaderStatusPort.TerminateBeingLeader>() {
////        @Override
////        public void handle(LeaderStatusPort.TerminateBeingLeader terminateBeingLeader) {
////            cancelHeartbeatTimeout();
////
////            resetLeader();
////            isConverged = false;
////            //higherUtilityNodes.clear();
////            deathVoteTimeout = null;
////
//////            PartitionId myPartitionId = new PartitionId(self.getPartitioningType(),
//////                    self.getPartitionIdDepth(), self.getPartitionId());
//////
//////            adjustDescriptorsToNewPartitionId(myPartitionId, higherUtilityNodes);
////        }
////    };
//
//    /**
//     * Counts the votes from its leader death election
//     */
//    private void evaluateDeathResponses() {
//
//        if (deathVoteTimeout != null) {
//            CancelTimeout timeout = new CancelTimeout(deathVoteTimeout);
//            trigger(timeout, timerPort);
//        }
//
//        // The leader is considered dead
//        if (leader != null
//                && leaderView != null
//                && deathMessageCounter >= leaderView.size() * config.getDeathVoteMajorityPercentage()
//                && aliveCounter < Math.ceil((float) leaderView.size() * config.getLeaderDeathMajorityPercentage())) {
//
//            for (SearchDescriptor searchDescriptor : leaderView) {
//                LeaderDeathAnnouncementMessage msg = new LeaderDeathAnnouncementMessage(self.getAddress(), searchDescriptor.getVodAddress(), leader);
//                trigger(msg, networkPort);
//            }
//
////            trigger(new NodeCrashEvent(leader.getVodAddress()), leaderStatusPort);
//            resetLeader();
//        } else { // The leader MIGHT be alive
//            leaderIsAlive = true;
//            scheduleHeartbeatTimeout(config.getHeartbeatWaitTimeout());
//        }
//
//        deathMessageCounter = 0;
//        aliveCounter = 0;
//    }
//
//    /**
//     * Starts the next heart beat timeout using a custom period
//     *
//     * @param delay
//     * custom period in ms
//     */
//    private void scheduleHeartbeatTimeout(int delay) {
//        ScheduleTimeout timeout = new ScheduleTimeout(delay);
//        timeout.setTimeoutEvent(new HeartbeatTimeout(timeout, self.getId()));
//        heartBeatTimeoutId = timeout.getTimeoutEvent().getTimeoutId();
//        trigger(timeout, timerPort);
//    }
//
//    /**
//     * Cancels the heart beat timeout
//     */
//    private void cancelHeartbeatTimeout() {
//        if (heartBeatTimeoutId != null) {
//            CancelTimeout ct = new CancelTimeout(heartBeatTimeoutId);
//            trigger(ct, timerPort);
//        }
//        heartBeatTimeoutId = null;
//    }
//
//    /**
//     * Accepts a certain leader
//     *
//     * @param node the leader's Address
//     * @param view the leader's current view
//     */
//    private void acceptLeader(SearchDescriptor node, Set<SearchDescriptor> view, PublicKey leaderPublicKey) {
//        leaderIsAlive = true;
//        leader = node;
//        leaderView = view;
//
//        cancelHeartbeatTimeout();
//        scheduleHeartbeatTimeout(config.getHeartbeatWaitTimeout());
//
////        trigger(new LeaderInfoUpdate(leader.getVodAddress(), leaderPublicKey), leaderStatusPort);
//    }
//
//    /**
//     * Rejects the leader
//     *
//     * @param node the leader's address
//     * @param betterNode the better node's descriptor
//     */
//    private void rejectLeader(VodAddress node, SearchDescriptor betterNode) {
//        resetLeader();
//
//        // Cancel old timeouts
//        cancelHeartbeatTimeout();
//        RejectLeaderMessage msg = new RejectLeaderMessage(self.getAddress(), node, betterNode);
//        trigger(msg, networkPort);
//    }
//
//    /**
//     * The nodes to be removed from the sample set as they are believed to be dead.
//     *
//     * @param nodesToRemove Set which is believed to be dead.
//     */
//    private void removeNodesFromLocalState(HashSet<VodAddress> nodesToRemove) {
//        for(VodAddress suspectedNode: nodesToRemove) {
//            removeNodeFromLocalState(suspectedNode);
//        }
//    }
//
//    private void removeNodeFromLocalState(VodAddress nodeAddress) {
//        removeNodeFromCollection(nodeAddress, leaderView);
//        removeNodeFromCollection(nodeAddress, higherUtilityNodes);
//    }
//
//    private void removeNodeFromCollection(VodAddress nodeAddress, Collection<SearchDescriptor> collection) {
//
//        if(collection != null) {
//            Iterator<SearchDescriptor> i = collection.iterator();
//            while (i.hasNext()) {
//                SearchDescriptor descriptor = i.next();
//
//                if (descriptor.getVodAddress().equals(nodeAddress)) {
//                    i.remove();
//                    break;
//                }
//            }
//        }
//    }
//
//    final Handler<FailureDetectorPort.FailureDetectorEvent> handleFailureDetector = new Handler<FailureDetectorPort.FailureDetectorEvent>() {
//
//        @Override
//        public void handle(FailureDetectorPort.FailureDetectorEvent event) {
//            removeNodesFromLocalState(event.getSuspectedNodes());
//        }
//    };
//
//
//    /**
//     * Reset the values identified with the leader.
//     */
//    void resetLeader() {
//
//        leader = null;
//        leaderView = null;
//        leaderIsAlive = false;
//
//        trigger(new LeaderInfoUpdate(null, null), leaderStatusPort);
//    }
//}