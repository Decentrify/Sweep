package se.sics.p2ptoolbox.election.core;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.timer.*;
import se.sics.gvod.timer.Timer;
import se.sics.kompics.*;
import se.sics.p2ptoolbox.croupier.api.util.CroupierPeerView;
import se.sics.p2ptoolbox.election.api.LCPeerView;
import se.sics.p2ptoolbox.election.api.LEContainer;
import se.sics.p2ptoolbox.election.api.msg.ElectionState;
import se.sics.p2ptoolbox.election.api.msg.LeaderState;
import se.sics.p2ptoolbox.election.api.msg.ViewUpdate;
import se.sics.p2ptoolbox.election.api.ports.LeaderElectionPort;
import se.sics.p2ptoolbox.election.core.data.LeaseCommit;
import se.sics.p2ptoolbox.election.core.data.Promise;
import se.sics.p2ptoolbox.election.core.msg.LeaderExtensionRequest;
import se.sics.p2ptoolbox.election.core.msg.LeaseCommitMessage;
import se.sics.p2ptoolbox.election.core.msg.LeaderPromiseMessage;
import se.sics.p2ptoolbox.election.core.util.ElectionHelper;
import se.sics.p2ptoolbox.election.core.util.PromiseResponseTracker;
import se.sics.p2ptoolbox.election.core.util.TimeoutCollection;
import se.sics.p2ptoolbox.gradient.api.GradientPort;
import se.sics.p2ptoolbox.gradient.api.msg.GradientSample;

import java.util.*;
import java.util.UUID;

/**
 * Leader Election Component.
 *  
 * This is the core component which is responsible for election and the maintenance of the leader in the system.
 * The nodes are constantly analyzing the samples from the sampling service and based on the convergence tries to assert themselves
 * as the leader.
 * 
 * <br/><br/>
 * 
 * In addition to this, this component works on leases in which the leader generates a lease 
 * and adds could happen only for that lease. The leader,  when the lease is on the verge of expiring tries to renew the lease by sending a special message to the
 * nodes in its view. 
 *   
 * <br/><br/>
 * 
 * <b>NOTE: </b> The lease needs to be short enough so that if the leader dies, the system is not in a transitive state.
 *
 * <b>CAUTION: </b> Under development, so unstable to use as it is.
 *
 */
public class ElectionLeader extends ComponentDefinition {

    Logger logger = LoggerFactory.getLogger(ElectionLeader.class);
    
    private LCPeerView selfLCView;
    private LEContainer selfLEContainer;
    private ElectionConfig config;
    private long seed;
    private VodAddress selfAddress;
    private Map<VodAddress, LEContainer> addressContainerMap;
    
    // Promise Sub Protocol.
    private UUID promiseRoundId;
    private TimeoutId promiseRoundTimeout;
    private PromiseResponseTracker promiseResponseTracker;
    
    // Convergence Variables.
    int convergenceCounter;
    boolean isConverged;
    boolean isUnderLease;

    // LE Container View.
    private SortedSet<LEContainer> higherUtilityNodes;
    private SortedSet<LEContainer> lowerUtilityNodes;

    private Comparator<LCPeerView> lcPeerViewComparator;
    private Comparator<LEContainer> leContainerComparator;

    private Comparator<CroupierPeerView> cpvUtilityComparator;
    
    // Ports.
    Positive<Timer> timerPositive = requires(Timer.class);
    Positive<VodNetwork> networkPositive = requires(VodNetwork.class);
    Negative<LeaderElectionPort> electionPort = provides(LeaderElectionPort.class);
    Positive<GradientPort> gradientPort = requires(GradientPort.class);
    
    
    public ElectionLeader(LeaderElectionInit init){
        
        doInit(init);
        subscribe(startHandler, control);
        subscribe(gradientSampleHandler, gradientPort);
        subscribe(viewUpdateHandler, electionPort);
        
        // Promise Subscriptions.
        subscribe(promiseResponseHandler, networkPositive);
        subscribe(promiseRoundTimeoutHandler, timerPositive);

        // Lease Subscriptions.
        subscribe(leaseTimeoutHandler, timerPositive);
    }
    
    
    // Init method.
    private void doInit(LeaderElectionInit init) {

        this.config = init.electionConfig;
        this.seed = init.seed;
        this.selfAddress = init.selfAddress;

        // voting protocol.
        isConverged = false;
        promiseResponseTracker = new PromiseResponseTracker();

        this.selfLCView = init.selfView;
        this.selfLEContainer = new LEContainer(selfAddress, selfLCView);
        this.addressContainerMap = new HashMap<VodAddress, LEContainer>();


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
            logger.debug("Started the Leader Election Component ...");
        }
    };





    Handler<ViewUpdate> viewUpdateHandler = new Handler<ViewUpdate>() {
        @Override
        public void handle(ViewUpdate viewUpdate) {

            selfLCView = viewUpdate.selfPv;
            selfLEContainer = new LEContainer(selfAddress, selfLCView);
        }
    };

    /**
     * Handler for the gradient sample that we receive from the gradient in the system.
     * Incorporate the gradient sample to recalculate the convergence and update the view of higher or lower utility nodes.
     */
    Handler<GradientSample> gradientSampleHandler = new Handler<GradientSample>() {
        @Override
        public void handle(GradientSample event) {
            
            logger.debug("{}: Received sample from gradient", selfAddress.getId());

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
            Pair<SortedSet<LEContainer>, SortedSet<LEContainer>> lowerAndHigherViewPair = ElectionHelper.getHigherAndLowerViews(addressContainerMap.values(), leContainerComparator, selfLEContainer);
            lowerUtilityNodes = lowerAndHigherViewPair.getValue0();
            higherUtilityNodes = lowerAndHigherViewPair.getValue1();

            // Check if the node is ready to be a leader.
            checkIfLeader();
        }
    };

    /**
     * The node after incorporating the sample, checks if it
     * is in a position to assert itself as a leader.
     */
    private void checkIfLeader() {
        
        if(addressContainerMap.size() < config.getViewSize()){
            logger.debug(" {}: I think I am leader but the view less than the minimum requirement, so returning.", selfAddress.getId());
            return;
        }

        // I don't see anybody above me, so should start voting.
        // Addition lease check is required because for the nodes which are in the node group will be acting under
        // lease of the leader, with special variable check.
        
        if(isConverged && higherUtilityNodes.size() == 0 && !isUnderLease){
            startVoting();
        }
    }


    /**
     * In case the node sees itself a candidate for being the leader, 
     * it initiates voting.
     */
    private void startVoting() {

        Promise.Request request = new Promise.Request(selfAddress, selfLCView);
        promiseRoundId = UUID.randomUUID();

        int leaderGroupSize = Math.min(config.getViewSize()/2 + 1, config.getMaxLeaderGroupSize());;
        Collection<LEContainer> leaderGroupNodes = createLeaderGroupNodes(leaderGroupSize);


        if(leaderGroupNodes.size() < leaderGroupSize){
            logger.error(" {} : Not asserting self as leader as the leader group size is less than required.", selfAddress.getId());
            return;
        }

        Collection<VodAddress> leaderGroupAddress = new ArrayList<VodAddress>();

        Iterator<LEContainer> iterator = lowerUtilityNodes.iterator();
        while(iterator.hasNext() && leaderGroupSize > 0){
            
            VodAddress lgMemberAddr = iterator.next().getSource();
            leaderGroupAddress.add(lgMemberAddr);
            
            LeaderPromiseMessage.Request promiseRequest = new LeaderPromiseMessage.Request(selfAddress, lgMemberAddr, promiseRoundId, request);
            trigger(promiseRequest, networkPositive);
            leaderGroupSize --;
        }

        promiseResponseTracker.startTracking(promiseRoundId, leaderGroupAddress);
        
        ScheduleTimeout st = new ScheduleTimeout(5000);
        st.setTimeoutEvent(new TimeoutCollection.PromiseRoundTimeout(st));
        promiseRoundTimeout = st.getTimeoutEvent().getTimeoutId();

        trigger(st, timerPositive);
    }

    
    Handler<LeaderPromiseMessage.Response> promiseResponseHandler = new Handler<LeaderPromiseMessage.Response>() {
        @Override
        public void handle(LeaderPromiseMessage.Response event) {

            logger.debug("{}: Received Promise Response from : {}", selfAddress.getId(), event.getSource().getId());
            int numPromises = promiseResponseTracker.addResponseAndGetSize(event);

            if(numPromises >= promiseResponseTracker.getLeaderGroupInformationSize()){

                // If all the nodes in leader group have replied.
                CancelTimeout cancelTimeout = new CancelTimeout(promiseRoundTimeout);
                trigger(cancelTimeout, timerPositive);

                if(promiseResponseTracker.isAccepted()){

                    logger.debug("{}: All the leader group nodes have promised.");
                    // IMPORTANT : Send the commit request with the same id as the promise, as the listening nodes are looking for this id only.
                    UUID commitRequestId = promiseResponseTracker.getRoundId();

                    for(VodAddress address : promiseResponseTracker.getLeaderGroupInformation()){
                        
                        LeaseCommit.Request requestContent = new LeaseCommit.Request(selfAddress, config.getPublicKey(), selfLCView);
                        LeaseCommitMessage.Request commitRequest = new LeaseCommitMessage.Request(selfAddress, address, commitRequestId, requestContent);
                        trigger(commitRequest, networkPositive);
                    }

                    // Leader Update.
                    trigger(new LeaderState.ElectedAsLeader(promiseResponseTracker.getLeaderGroupInformation()), electionPort);

                    // Lease Update.
                    isUnderLease = true;
                    trigger(new ElectionState.EnableLGMembership(), electionPort);

                    // Trigger the lease timeout.
                    ScheduleTimeout st = new ScheduleTimeout(config.getLeaseTime());
                    st.setTimeoutEvent(new TimeoutCollection.LeaseTimeout(st));

                    trigger(st, networkPositive);

                }
                // Reset the tracker information to prevent the behaviour again and again.
                promiseResponseTracker.resetTracker();
            }
        }
    };
    
    
    Handler<TimeoutCollection.PromiseRoundTimeout> promiseRoundTimeoutHandler = new Handler<TimeoutCollection.PromiseRoundTimeout>() {
        @Override
        public void handle(TimeoutCollection.PromiseRoundTimeout event) {
            logger.debug("{}: Promise Round Timed out. Resetting the tracker ... ", selfAddress.getId());
            promiseResponseTracker.resetTracker();
        }
    };

    /**
     * Lease for the current round timed out.
     * Now we need to reset some parameters in order to let other nodes to try and assert themselves as leader.
     */

    Handler<TimeoutCollection.LeaseTimeout> leaseTimeoutHandler = new Handler<TimeoutCollection.LeaseTimeout>() {
        @Override
        public void handle(TimeoutCollection.LeaseTimeout event) {

            logger.debug(" Lease Timed out at Leader End: {} , trying to extend the lease", selfAddress.getId());
            if(isExtensionPossible()){

                logger.debug("Trying to extend the leadership.");

                int leaderGroupSize = Math.min(config.getViewSize()/2 + 1, config.getMaxLeaderGroupSize());;
                Collection<LEContainer> lowerNodes = createLeaderGroupNodes(leaderGroupSize);

                if(lowerNodes.size() < leaderGroupSize){

                    logger.error("{}: Terminate being the leader as state seems to be corrupted.");
                    terminateBeingLeader();
                    return;
                }

                for(LEContainer container : lowerNodes){
                    trigger(new LeaderExtensionRequest(selfAddress, container.getSource(), UUID.randomUUID(), new LeaseCommit.Request(selfAddress, config.getPublicKey(), selfLCView)), networkPositive);
                }

                // Extend the lease.
                ScheduleTimeout st = new ScheduleTimeout(config.getLeaseTime());
                st.setTimeoutEvent(new TimeoutCollection.LeaseTimeout(st));
                trigger(st, timerPositive);

            }
            else{
                terminateBeingLeader();
            }
        }
    };


    /**
     * In case the leader sees some node above himself, then
     * keeping in mind the fairness policy the node should terminate.
     */
    private void terminateBeingLeader(){

        // Disable leadership and membership.
        isUnderLease = false;
        trigger(new LeaderState.TerminateBeingLeader(), electionPort);
        trigger(new ElectionState.DisableLGMembership(), electionPort);
    }



    /**
     * Check if the leader is still suited to be the leader.
     *
     * @return extension possible
     */
    private boolean isExtensionPossible(){

        boolean extensionPossible = true;

        if(addressContainerMap.size() < config.getViewSize()){
            extensionPossible = false;
        }

        else {

            SortedSet<LCPeerView> updatedSortedContainerSet = new TreeSet<LCPeerView>(lcPeerViewComparator);

            for(LEContainer container : addressContainerMap.values()){
                updatedSortedContainerSet.add(container.getLCPeerView().disableLGMembership());
            }
            LCPeerView updatedTempSelfView = selfLCView.disableLGMembership();

            if(updatedSortedContainerSet.tailSet(updatedTempSelfView).size() != 0){
                extensionPossible = false;
            }
        }

        return extensionPossible;
    }


    /**
     * Construct a collection of nodes which the leader thinks should be in the leader group
     * @param size size of the leader group
     *
     * @return Leader Group Collection.
     */
    private Collection<LEContainer> createLeaderGroupNodes (int size){

        Collection<LEContainer> collection = new ArrayList<LEContainer>();

        if(size <= lowerUtilityNodes.size()){

            Iterator<LEContainer> iterator = lowerUtilityNodes.iterator();
            while(iterator.hasNext() && size > 0){

                collection.add(iterator.next());
                size --;
            }
        }

        return collection;
    }




    /**
     * Initialization Class for the Leader Election Component.
     */
    public static class LeaderElectionInit extends Init<ElectionLeader>{
        
        VodAddress selfAddress;
        LCPeerView selfView;
        long seed;
        ElectionConfig electionConfig;
        
        public LeaderElectionInit(VodAddress selfAddress, LCPeerView selfView, long seed, ElectionConfig electionConfig){
            this.selfAddress = selfAddress;
            this.selfView = selfView;
            this.seed = seed;
            this.electionConfig = electionConfig;
        }
    }
}
