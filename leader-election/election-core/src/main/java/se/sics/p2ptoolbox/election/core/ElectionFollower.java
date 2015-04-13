package se.sics.p2ptoolbox.election.core;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.timer.*;
import se.sics.kompics.*;
import se.sics.p2ptoolbox.election.api.LCPeerView;
import se.sics.p2ptoolbox.election.api.LEContainer;
import se.sics.p2ptoolbox.election.api.msg.ElectionState;
import se.sics.p2ptoolbox.election.api.msg.LeaderUpdate;
import se.sics.p2ptoolbox.election.api.msg.ViewUpdate;
import se.sics.p2ptoolbox.election.api.msg.mock.MockedGradientUpdate;
import se.sics.p2ptoolbox.election.api.ports.LeaderElectionPort;
import se.sics.p2ptoolbox.election.api.ports.TestPort;
import se.sics.p2ptoolbox.election.core.data.LeaseCommitUpdated;
import se.sics.p2ptoolbox.election.core.data.Promise;
import se.sics.p2ptoolbox.election.core.msg.LeaderExtensionRequest;
import se.sics.p2ptoolbox.election.core.msg.LeaderPromiseMessage;
import se.sics.p2ptoolbox.election.core.msg.LeaseCommitMessage;
import se.sics.p2ptoolbox.election.core.msg.LeaseCommitMessageUpdated;
import se.sics.p2ptoolbox.election.core.util.ElectionHelper;
import se.sics.p2ptoolbox.election.core.util.LeaderFilter;
import se.sics.p2ptoolbox.election.core.util.TimeoutCollection;
import se.sics.p2ptoolbox.gradient.api.GradientPort;
import se.sics.p2ptoolbox.gradient.api.msg.GradientSample;

import java.util.*;
import java.util.UUID;

/**
 * Election Follower part of the Leader Election Protocol.
 */
public class ElectionFollower extends ComponentDefinition {

    Logger logger = LoggerFactory.getLogger(ElectionFollower.class);

    VodAddress selfAddress;
    LCPeerView selfLCView;
    LEContainer selfContainer;
    private LeaderFilter filter;

    private ElectionConfig config;
    private SortedSet<LEContainer> higherUtilityNodes;
    private SortedSet<LEContainer> lowerUtilityNodes;

    private Comparator<LCPeerView> lcPeerViewComparator;
    private Comparator<LEContainer> leContainerComparator;

    // Gradient Sample.
    int convergenceCounter = 0;
    private boolean isConverged;
    private Map<VodAddress, LEContainer> addressContainerMap;

    // Leader Election.
    private UUID electionRoundId;
    private boolean inElection;

    private TimeoutId awaitLeaseCommitId;
    private TimeoutId leaseTimeoutId;
    private VodAddress leaderAddress;

    // Ports.
    Positive<VodNetwork> networkPositive = requires(VodNetwork.class);
    Positive<se.sics.gvod.timer.Timer> timerPositive = requires(se.sics.gvod.timer.Timer.class);
    Positive<GradientPort> gradientPort = requires(GradientPort.class);
    Negative<LeaderElectionPort> electionPort = provides(LeaderElectionPort.class);
    Negative<TestPort> testPortNegative = provides(TestPort.class);

    public ElectionFollower(ElectionInit<ElectionFollower> init) {

        doInit(init);

        subscribe(startHandler, control);
        subscribe(gradientSampleHandler, gradientPort);
        subscribe(viewUpdateHandler, electionPort);

        subscribe(mockedUpdateHandler, testPortNegative);

        subscribe(promiseRequestHandler, networkPositive);
        subscribe(awaitLeaseCommitTimeoutHandler, timerPositive);
        subscribe(leaseCommitRequestHandlerUpdated, networkPositive);
        subscribe(leaseTimeoutHandler, timerPositive);

        subscribe(leaderExtensionRequestHandler, networkPositive);
    }

    private void doInit(ElectionInit<ElectionFollower> init) {

        config = init.electionConfig;
        selfAddress = init.selfAddress;
        addressContainerMap = new HashMap<VodAddress, LEContainer>();

        selfLCView = init.initialView;
        selfContainer = new LEContainer(selfAddress, selfLCView);
        filter = init.filter;

        lcPeerViewComparator = init.comparator;
        this.leContainerComparator = new Comparator<LEContainer>() {
            @Override
            public int compare(LEContainer o1, LEContainer o2) {

                if (o1 == null || o2 == null) {
                    throw new IllegalArgumentException("Can't compare null values");
                }

                LCPeerView view1 = o1.getLCPeerView();
                LCPeerView view2 = o2.getLCPeerView();

                return lcPeerViewComparator.compare(view1, view2);
            }
        };

        lowerUtilityNodes = new TreeSet<LEContainer>(leContainerComparator);
        higherUtilityNodes = new TreeSet<LEContainer>(leContainerComparator);
    }


    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            logger.trace("{}: Election Follower Component is up.", selfAddress.getId());
        }
    };


    Handler<MockedGradientUpdate> mockedUpdateHandler = new Handler<MockedGradientUpdate>() {
        @Override
        public void handle(MockedGradientUpdate event) {
            logger.trace("Received mocked update from the gradient");

            // Incorporate the new sample.
            Map<VodAddress, LEContainer> oldContainerMap = addressContainerMap;
            addressContainerMap = ElectionHelper.addGradientSample(event.cpvCollection);

            // Check how much the sample changed.
            if (ElectionHelper.isRoundConverged(oldContainerMap.keySet(), addressContainerMap.keySet(), config.getConvergenceTest())) {
                if (!isConverged) {

                    convergenceCounter++;
                    if (convergenceCounter >= config.getConvergenceRounds()) {
                        isConverged = true;
                    }
                }
            } else {
                convergenceCounter = 0;
                if (isConverged) {
                    isConverged = false;
                }
            }

            // Update the views.
            Pair<SortedSet<LEContainer>, SortedSet<LEContainer>> lowerAndHigherViewPair = ElectionHelper.getHigherAndLowerViews(addressContainerMap.values(), leContainerComparator, selfContainer);
            lowerUtilityNodes = lowerAndHigherViewPair.getValue0();
            higherUtilityNodes = lowerAndHigherViewPair.getValue1();

        }
    };


    Handler<GradientSample> gradientSampleHandler = new Handler<GradientSample>() {
        @Override
        public void handle(GradientSample event) {

            logger.trace("{}: Received gradient sample", selfAddress.getId());

            // Incorporate the new sample.
            Map<VodAddress, LEContainer> oldContainerMap = addressContainerMap;
            addressContainerMap = ElectionHelper.addGradientSample(event.gradientSample);

            // Check how much the sample changed.
            if (ElectionHelper.isRoundConverged(oldContainerMap.keySet(), addressContainerMap.keySet(), config.getConvergenceTest())) {

                if (!isConverged) {

                    convergenceCounter++;
                    if (convergenceCounter >= config.getConvergenceRounds()) {
                        isConverged = true;
                    }
                }

            } else {
                convergenceCounter = 0;
                if (isConverged) {
                    isConverged = false;
                }
            }

            // Update the views.
            Pair<SortedSet<LEContainer>, SortedSet<LEContainer>> lowerAndHigherViewPair = ElectionHelper.getHigherAndLowerViews(addressContainerMap.values(), leContainerComparator, selfContainer);
            lowerUtilityNodes = lowerAndHigherViewPair.getValue0();
            higherUtilityNodes = lowerAndHigherViewPair.getValue1();

        }
    };


    Handler<ViewUpdate> viewUpdateHandler = new Handler<ViewUpdate>() {
        @Override
        public void handle(ViewUpdate viewUpdate) {


            LCPeerView oldView = selfLCView;
            selfLCView = viewUpdate.selfPv;
            selfContainer = new LEContainer(selfAddress, selfLCView);

            // Resetting of election information happens naturally, when nobody extends the lease from the leader side.

//            if (filter.terminateLeader(oldView, selfLCView)) {
//
//                logger.debug("{}: Terminate the election information.", selfAddress.getId());
//
//                if (leaseTimeoutId != null) {
//                    CancelTimeout ct = new CancelTimeout(leaseTimeoutId);
//                    trigger(ct, timerPositive);
//                }
//                terminateElectionInformation();
//            }

            // Resetting the in election check is kind of tricky so need to be careful about the procedure.
            if (viewUpdate.electionRoundId != null && inElection) {
                if (electionRoundId != null && electionRoundId.equals(viewUpdate.electionRoundId)) {
                    logger.warn("{}: Resetting election check for the UUID: {}", selfAddress.getId(), electionRoundId);
                    
                    inElection = false;
                    resetElectionMetaData();
                }
            }
        }
    };


    /**
     * Promise request from the node trying to assert itself as leader.
     * The request is sent to all the nodes in the system that the originator of request seems fit
     * and wants them to be a part of the leader group.
     */
    Handler<LeaderPromiseMessage.Request> promiseRequestHandler = new Handler<LeaderPromiseMessage.Request>() {
        @Override
        public void handle(LeaderPromiseMessage.Request event) {


            logger.warn("{}: Received promise request from : {}", selfAddress.getId(), event.getSource().getId());
            
            LeaderPromiseMessage.Response response;
            LCPeerView requestLeaderView = event.content.leaderView;

            boolean acceptCandidate = true;

            if (selfLCView.isLeaderGroupMember() || inElection) {
                // If part of leader group or already promised by being present in election, I deny promise.
                acceptCandidate = false;
            } else {

                if (event.content.leaderAddress.getPeerAddress().equals(selfAddress.getPeerAddress())) {
                    acceptCandidate = true; // Always accept self.
                    
                } else {

                    LCPeerView nodeToCompareTo = getHighestUtilityNode();
                    if (lcPeerViewComparator.compare(requestLeaderView, nodeToCompareTo) < 0) {
                        acceptCandidate = false;
                    }
                }
            }

            // Update the election round id only if I decide to accept the candidate.
            if(acceptCandidate){
                
                electionRoundId = event.content.electionRoundId;
                inElection = true;

                ScheduleTimeout st = new ScheduleTimeout(5000);
                st.setTimeoutEvent(new TimeoutCollection.AwaitLeaseCommitTimeout(st, electionRoundId));

                awaitLeaseCommitId = st.getTimeoutEvent().getTimeoutId();
                trigger(st, timerPositive);
            }
            response = new LeaderPromiseMessage.Response(selfAddress, event.getVodSource(), event.id, new Promise.Response(acceptCandidate, isConverged, event.content.electionRoundId));
            trigger(response, networkPositive);
        }
    };


    /**
     * Timeout Handler in case the node that had earlier extracted a promise from this node
     * didn't answer with the commit message in time.
     */
    Handler<TimeoutCollection.AwaitLeaseCommitTimeout> awaitLeaseCommitTimeoutHandler = new Handler<TimeoutCollection.AwaitLeaseCommitTimeout>() {
        @Override
        public void handle(TimeoutCollection.AwaitLeaseCommitTimeout event) {

            logger.warn("{}: The promise is not yet fulfilled with lease commit", selfAddress.getId());

            if (awaitLeaseCommitId != null && awaitLeaseCommitId.equals(event.getTimeoutId())) {
                
                // Might be triggered even if the response is handled.
                inElection = false;
                electionRoundId = null;
                trigger(new ElectionState.DisableLGMembership(event.electionRoundId), electionPort);
                
            } else {
                logger.warn("Timeout triggered even though the cancel was sent.");
            }

        }
    };


    /**
     * Reset the meta data associated with the current election algorithm.
     */
    private void resetElectionMetaData() {
        electionRoundId = null;
    }


    /**
     * Received the lease commit request from the node trying to assert itself as leader.
     * Accept the request in case it is from the same round id.
     */
    Handler<LeaseCommitMessageUpdated.Request> leaseCommitRequestHandlerUpdated = new Handler<LeaseCommitMessageUpdated.Request>() {
        @Override
        public void handle(LeaseCommitMessageUpdated.Request event) {

            logger.warn("{}: Received lease commit request from: {}", selfAddress.getId(), event.getVodSource().getId());
            LeaseCommitUpdated.Response response;

            if (electionRoundId == null || !electionRoundId.equals(event.content.electionRoundId)) {

                logger.warn("{}: Received an election response for the round id which has expired or timed out.", selfAddress.getId());
                response = new LeaseCommitUpdated.Response(false, event.content.electionRoundId);
                trigger(new LeaseCommitMessageUpdated.Response(selfAddress, event.getVodSource(), event.id, response), networkPositive);
            } else {

                response = new LeaseCommitUpdated.Response(true, event.content.electionRoundId);
                trigger(new LeaseCommitMessageUpdated.Response(selfAddress, event.getVodSource(), event.id, response), networkPositive);

                // Cancel the existing awaiting for lease commit timeout.
                CancelTimeout timeout = new CancelTimeout(awaitLeaseCommitId);
                trigger(timeout, timerPositive);
                awaitLeaseCommitId = null;

                logger.warn("{}: My new leader: {}", selfAddress.getId(), event.content.leaderAddress);
                leaderAddress = event.content.leaderAddress;

                trigger(new ElectionState.EnableLGMembership(electionRoundId), electionPort);
                trigger(new LeaderUpdate(event.content.leaderPublicKey, event.content.leaderAddress), electionPort);

                ScheduleTimeout st = new ScheduleTimeout(config.getFollowerLeaseTime());
                st.setTimeoutEvent(new TimeoutCollection.LeaseTimeout(st));

                leaseTimeoutId = st.getTimeoutEvent().getTimeoutId();
                trigger(st, timerPositive);

            }
        }
    };


    /**
     * As soon as the lease expires reset all the parameters related to the node being under lease.
     * CHECK : If we also need to reset the parameters associated with the current leader. (YES I THINK SO)
     */
    Handler<TimeoutCollection.LeaseTimeout> leaseTimeoutHandler = new Handler<TimeoutCollection.LeaseTimeout>() {
        @Override
        public void handle(TimeoutCollection.LeaseTimeout event) {

            if(leaseTimeoutId != null && leaseTimeoutId.equals(event.getTimeoutId())){
                
                logger.warn("{}: Special : Lease timed out.", selfAddress.getId());
                terminateElectionInformation();
            }
            else{
                logger.warn("{}: Application current lease timeout id has changed");
            }

        }
    };

    /**
     * In case the lease times out or the application demands it, reset the
     * election state in the component.
     * <p/>
     * But do not be quick to dismiss the leader information and then send a leader update to the application.
     * In case I am no longer part of the leader group, then I should eventually pull the information about the current leader.
     */
    private void terminateElectionInformation() {

        leaderAddress = null;
        resetElectionMetaData();
        trigger(new ElectionState.DisableLGMembership(null), electionPort); // round is doesnt matter at this moment.
    }


    /**
     * Leader Extension request received. Node which is currently the leader is trying to reassert itself as the leader again.
     * The protocol with the extension simply follows is that the leader should be allowed to continue as leader.
     */
    Handler<LeaderExtensionRequest> leaderExtensionRequestHandler = new Handler<LeaderExtensionRequest>() {

        @Override
        public void handle(LeaderExtensionRequest leaderExtensionRequest) {

            logger.warn("{}: Received leader extension request from the node: {}", selfAddress.getId(), leaderExtensionRequest.getVodSource().getId());
            if (selfLCView.isLeaderGroupMember()) {

                if (leaderAddress != null && !leaderExtensionRequest.content.leaderAddress.equals(leaderAddress)) {
                    logger.warn("{}: There might be a problem with the leader extension or a special case as I received lease extension from the node other than current leader that I already has set. ", selfAddress.getId());
                }

                CancelTimeout cancelTimeout = new CancelTimeout(leaseTimeoutId);
                trigger(cancelTimeout, timerPositive);
            }

            // Prevent the edge case in which the node which is under a lease might take time
            // to get a response back from the application representing the information and therefore in that time something fishy can happen.
            inElection = true;
            electionRoundId = leaderExtensionRequest.content.electionRoundId;
            trigger(new ElectionState.EnableLGMembership(electionRoundId), electionPort);

            // Inform the component listening about the leader and schedule a new lease.
            trigger(new LeaderUpdate(leaderExtensionRequest.content.leaderPublicKey, leaderExtensionRequest.content.leaderAddress), electionPort);

            ScheduleTimeout st = new ScheduleTimeout(config.getFollowerLeaseTime());
            st.setTimeoutEvent(new TimeoutCollection.LeaseTimeout(st));
            leaseTimeoutId = st.getTimeoutEvent().getTimeoutId();

            trigger(st, timerPositive);
        }
    };


    /**
     * Analyze the views present locally and
     * return the highest utility node.
     *
     * @return Highest utility Node.
     */
    public LCPeerView getHighestUtilityNode() {

        if (higherUtilityNodes.size() != 0) {
            return higherUtilityNodes.last().getLCPeerView();
        }

        return selfLCView;
    }

}
