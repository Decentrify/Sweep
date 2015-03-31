package se.sics.p2ptoolbox.election.core;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.timer.CancelTimeout;
import se.sics.gvod.timer.ScheduleTimeout;
import se.sics.gvod.timer.Timeout;
import se.sics.gvod.timer.TimeoutId;
import se.sics.kompics.*;
import se.sics.kompics.timer.Timer;
import se.sics.p2ptoolbox.croupier.api.util.CroupierPeerView;
import se.sics.p2ptoolbox.croupier.api.util.PeerView;
import se.sics.p2ptoolbox.election.api.LCPeerView;
import se.sics.p2ptoolbox.election.api.LEContainer;
import se.sics.p2ptoolbox.election.api.msg.LeaderUpdate;
import se.sics.p2ptoolbox.election.api.msg.ViewUpdate;
import se.sics.p2ptoolbox.election.api.ports.LeaderElectionPort;
import se.sics.p2ptoolbox.election.core.data.Promise;
import se.sics.p2ptoolbox.election.core.msg.LeaderPromiseMessage;
import se.sics.p2ptoolbox.election.core.msg.LeaseCommitMessage;
import se.sics.p2ptoolbox.election.core.util.ElectionHelper;
import se.sics.p2ptoolbox.election.core.util.TimeoutCollection;
import se.sics.p2ptoolbox.gradient.api.GradientPort;
import se.sics.p2ptoolbox.gradient.api.msg.GradientSample;

import java.util.*;

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
    
    // Ports.
    Positive<VodNetwork> networkPositive = requires(VodNetwork.class);
    Positive<Timer> timerPositive = requires(Timer.class);
    Positive<GradientPort> gradientPort = requires(GradientPort.class);
    Negative<LeaderElectionPort> electionPort = provides(LeaderElectionPort.class);


    public ElectionFollower(ElectionFollowerInit init){
        
        doInit(init);

        subscribe(startHandler, control);
        subscribe(gradientSampleHandler, gradientPort);
        subscribe(viewUpdateHandler, electionPort);
        
        subscribe(promiseRequestHandler, networkPositive);
        subscribe(awaitLeaseCommitTimeoutHandler, timerPositive);
        subscribe(leaseCommitRequestHandler, networkPositive);
        subscribe(leaseTimeoutHandler, networkPositive);
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
            logger.debug(" Started the election follower component with address: {}", selfAddress);
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
    Handler<LeaseCommitMessage.Request> leaseCommitRequestHandler = new Handler<LeaseCommitMessage.Request>() {
        @Override
        public void handle(LeaseCommitMessage.Request event) {

            logger.debug("{}: Received lease commit message request from : {}", selfAddress.getId(), event.getSource().getId());

            if(electionRoundId == null || !electionRoundId.equals(event.id)){
                logger.warn("{}: Received an election response for the round id which has expired or timed out.", selfAddress.getId());
                return;
            }

            // Cancel the existing awaiting for lease commit timeout.
            CancelTimeout timeout = new CancelTimeout(awaitLeaseCommitId);
            trigger(timeout, networkPositive);
            
            
            // Steps:
            // 1) Enable the leader group inclusion check.
            // 2) Start the lease timeout.
            
            // Updating the lease parameters.
            isUnderLease = true;
            electionRoundId = null;

            ScheduleTimeout st = new ScheduleTimeout(config.getLeaseTime());
            st.setTimeoutEvent(new TimeoutCollection.LeaseTimeout(st));

            leaseTimeoutId = st.getTimeoutEvent().getTimeoutId();
            trigger(st, networkPositive);

        }
    };
    
    
    
    Handler<TimeoutCollection.LeaseTimeout> leaseTimeoutHandler = new Handler<TimeoutCollection.LeaseTimeout>() {
        @Override
        public void handle(TimeoutCollection.LeaseTimeout event) {
            logger.debug("{}: Lease timed out.", selfAddress.getId());
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
