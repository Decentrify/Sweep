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
import se.sics.p2ptoolbox.election.core.data.Promise;
import se.sics.p2ptoolbox.election.core.msg.LeaderExtensionRequest;
import se.sics.p2ptoolbox.election.core.msg.LeaderPromiseMessage;
import se.sics.p2ptoolbox.election.core.msg.LeaseCommitMessage;
import se.sics.p2ptoolbox.election.core.util.ElectionHelper;
import se.sics.p2ptoolbox.election.core.util.TimeoutCollection;
import se.sics.p2ptoolbox.gradient.api.GradientPort;
import se.sics.p2ptoolbox.gradient.api.msg.GradientSample;

import java.security.PublicKey;
import java.util.*;
import java.util.Timer;
import java.util.UUID;

/**
 * Election Follower part of the Leader Election Protocol.
 * 
 */
public class ElectionFollower extends ComponentDefinition{

    Logger logger = LoggerFactory.getLogger(ElectionFollower.class);
    
    VodAddress selfAddress;
    LCPeerView selfLCView;
    LEContainer selfContainer;

    private ElectionConfig config;
    private SortedSet<LEContainer> higherUtilityNodes;
    private SortedSet<LEContainer> lowerUtilityNodes;

    private Comparator<LCPeerView> lcPeerViewComparator;
    private Comparator<LEContainer> leContainerComparator;

    // Gradient Sample.
    int convergenceCounter =0;
    private boolean isConverged;
    private Map<VodAddress, LEContainer> addressContainerMap;

    // Leader Election.
    private UUID electionRoundId;
    private boolean isUnderLease;
    private TimeoutId awaitLeaseCommitId;
    private TimeoutId leaseTimeoutId;
    private VodAddress leaderAddress;
    private PublicKey leaderPublicKey;
    
    // Ports.
    Positive<VodNetwork> networkPositive = requires(VodNetwork.class);
    Positive<se.sics.gvod.timer.Timer> timerPositive = requires(se.sics.gvod.timer.Timer.class);
    Positive<GradientPort> gradientPort = requires(GradientPort.class);
    Negative<LeaderElectionPort> electionPort = provides(LeaderElectionPort.class);
    Negative<TestPort> testPortNegative = provides(TestPort.class);

    public ElectionFollower(ElectionFollowerInit init){
        
        doInit(init);

        subscribe(startHandler, control);
        subscribe(gradientSampleHandler, gradientPort);
        subscribe(viewUpdateHandler, electionPort);

        subscribe(mockedUpdateHandler, testPortNegative);

        subscribe(promiseRequestHandler, networkPositive);
        subscribe(awaitLeaseCommitTimeoutHandler, timerPositive);
        subscribe(leaseCommitRequestHandler, networkPositive);
        subscribe(leaseTimeoutHandler, timerPositive);

        subscribe(leaderExtensionRequestHandler, networkPositive);
    }

    private void doInit(ElectionFollowerInit init) {

        config = init.electionConfig;
        selfAddress = init.selfAddress;
        addressContainerMap = new HashMap<VodAddress, LEContainer>();

        selfLCView = init.selfView;
        selfContainer = new LEContainer(selfAddress, selfLCView);

        lcPeerViewComparator = config.getUtilityComparator();
        this.leContainerComparator = new Comparator<LEContainer>() {
            @Override
            public int compare(LEContainer o1, LEContainer o2) {

                if(o1 == null || o2 == null){
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
            if(ElectionHelper.isRoundConverged(oldContainerMap.keySet(), addressContainerMap.keySet(), config.getConvergenceTest())) {
                convergenceCounter++;
                if (convergenceCounter > config.getConvergenceRounds()) {
                    isConverged = true;
                }
            }
            else{
                convergenceCounter = 0;
                if(isConverged){
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
            
            logger.debug("{}: Received gradient sample");

            // Incorporate the new sample.
            Map<VodAddress, LEContainer> oldContainerMap = addressContainerMap;
            addressContainerMap = ElectionHelper.addGradientSample(event.gradientSample);

            // Check how much the sample changed.
            if(ElectionHelper.isRoundConverged(oldContainerMap.keySet(), addressContainerMap.keySet(), config.getConvergenceTest())) {
                convergenceCounter++;
                if (convergenceCounter > config.getConvergenceRounds()) {
                    isConverged = true;
                }
            }
            else{
                convergenceCounter = 0;
                if(isConverged){
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

            selfLCView = viewUpdate.selfPv;
            selfContainer = new LEContainer(selfAddress, selfLCView);
        }
    };


    /**
     * Promise request from the node trying to assert itself as leader.
     * The request is sent to all the nodes in the system that the originator of request seems fit
     * and wants them to be a part of the leader group.
     *
     */
    Handler<LeaderPromiseMessage.Request> promiseRequestHandler = new Handler<LeaderPromiseMessage.Request>() {
        @Override
        public void handle(LeaderPromiseMessage.Request event) {

            logger.debug("{}: Received promise request from : {}", selfAddress.getId(), event.getSource().getId());

            LeaderPromiseMessage.Response response;
            LCPeerView requestLeaderView = event.content.leaderView;

            boolean acceptCandidate = true;

            if(isUnderLease || (electionRoundId != null)) {
                // If part of leader group or already promised by setting election round id, I deny promise.
                acceptCandidate = false;
            }
            
            else{
                
                LCPeerView nodeToCompareTo = getHighestUtilityNode();
                if(lcPeerViewComparator.compare(requestLeaderView, nodeToCompareTo) >= 0){

                    electionRoundId = event.id;
                    ScheduleTimeout st = new ScheduleTimeout(3000);
                    st.setTimeoutEvent(new TimeoutCollection.AwaitLeaseCommitTimeout(st));

                    awaitLeaseCommitId = st.getTimeoutEvent().getTimeoutId();
                    trigger(st, timerPositive);
                }
                else
                    acceptCandidate = false;
            }

            response = new LeaderPromiseMessage.Response(selfAddress, event.getVodSource(), event.id, new Promise.Response(acceptCandidate, isConverged));
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
            logger.debug("{}: The promise is not yet fulfilled with lease commit", selfAddress.getId());
            electionRoundId = null;
        }
    };



    /**
     * Received the lease commit request from the node trying to assert itself as leader.
     */
    Handler<LeaseCommitMessage> leaseCommitRequestHandler = new Handler<LeaseCommitMessage>() {
        @Override
        public void handle(LeaseCommitMessage event) {

            logger.debug("{}: Received lease commit message request from : {}", selfAddress.getId(), event.getSource().getId());

            if(electionRoundId == null || !electionRoundId.equals(event.id)){
                logger.warn("{}: Received an election response for the round id which has expired or timed out.", selfAddress.getId());
                return;
            }

            // Cancel the existing awaiting for lease commit timeout.
            CancelTimeout timeout = new CancelTimeout(awaitLeaseCommitId);
            trigger(timeout, timerPositive);

            logger.debug("{}: My new leader: {}", selfAddress.getId(), event.content.leaderAddress);

            isUnderLease = true;
            trigger(new ElectionState.EnableLGMembership(), electionPort);
            trigger(new LeaderUpdate(event.content.leaderPublicKey, event.content.leaderAddress), electionPort);


            ScheduleTimeout st = new ScheduleTimeout(config.getLeaseTime());
            st.setTimeoutEvent(new TimeoutCollection.LeaseTimeout(st));

            leaseTimeoutId = st.getTimeoutEvent().getTimeoutId();
            trigger(st, timerPositive);

        }
    };


    /**
     * As soon as the lease expires reset all the parameters related to the node being under lease.
     * CHECK : If we also need to reset the parameters associated with the current leader. (YES I THINK SO)
     *
     */
    Handler<TimeoutCollection.LeaseTimeout> leaseTimeoutHandler = new Handler<TimeoutCollection.LeaseTimeout>() {
        @Override
        public void handle(TimeoutCollection.LeaseTimeout event) {

            logger.debug("{}: Special : Lease timed out.", selfAddress.getId());
            isUnderLease = false;
            electionRoundId = null;

            trigger(new ElectionState.DisableLGMembership(), electionPort);
        }
    };


    /**
     * Leader Extension request received. Node which is currently the leader is trying to reassert itself as the leader again.
     * The protocol with the extension simply follows is that the leader should be allowed to continue as leader.
     */
    Handler<LeaderExtensionRequest> leaderExtensionRequestHandler = new Handler<LeaderExtensionRequest>() {

        @Override
        public void handle(LeaderExtensionRequest leaderExtensionRequest) {
            logger.debug("{}: Received leader extension request from the node: {}", selfAddress.getId(), leaderExtensionRequest.getVodSource().getId());

            if(isUnderLease){

                if(leaderAddress != null && !leaderExtensionRequest.content.leaderAddress.equals(leaderAddress)){
                    logger.warn("{}: There might be a problem with the leader extension as I received lease extension from the node other than current leader. ", selfAddress.getId());
                }

                CancelTimeout cancelTimeout = new CancelTimeout(leaseTimeoutId);
                trigger(cancelTimeout, timerPositive);
            }

            else{

                isUnderLease = true;
                trigger(new ElectionState.EnableLGMembership(), electionPort);
            }

            // Inform the component listening about the leader and schedule a new lease.
            trigger(new LeaderUpdate(leaderExtensionRequest.content.leaderPublicKey, leaderExtensionRequest.content.leaderAddress), electionPort);

            ScheduleTimeout st = new ScheduleTimeout(config.getLeaseTime());
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
    public LCPeerView getHighestUtilityNode(){

        if(higherUtilityNodes.size() != 0){
            return higherUtilityNodes.last().getLCPeerView();
        }
        
        return selfLCView;
    }


    /**
     * Capture the information related to the current leader in the system.
     */
    public void storeCurrentLeaderInformation(){

    }

    /**
     * Initialization Class for the Leader Election Component.
     */
    public static class ElectionFollowerInit extends Init<ElectionFollower> {

        VodAddress selfAddress;
        LCPeerView selfView;
        long seed;
        ElectionConfig electionConfig;

        public ElectionFollowerInit(VodAddress selfAddress, LCPeerView selfView, long seed, ElectionConfig electionConfig){
            this.selfAddress = selfAddress;
            this.selfView = selfView;
            this.seed = seed;
            this.electionConfig = electionConfig;
        }
    }
    
}
