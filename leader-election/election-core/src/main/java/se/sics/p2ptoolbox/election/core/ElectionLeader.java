package se.sics.p2ptoolbox.election.core;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.timer.*;
import se.sics.gvod.timer.Timer;
import se.sics.kompics.*;
import se.sics.p2ptoolbox.election.api.LCPeerView;
import se.sics.p2ptoolbox.election.api.LEContainer;
import se.sics.p2ptoolbox.election.api.msg.LeaderState;
import se.sics.p2ptoolbox.election.api.msg.ViewUpdate;
import se.sics.p2ptoolbox.election.api.msg.mock.MockedGradientUpdate;
import se.sics.p2ptoolbox.election.api.ports.LeaderElectionPort;
import se.sics.p2ptoolbox.election.api.ports.TestPort;
import se.sics.p2ptoolbox.election.core.data.ExtensionRequest;
import se.sics.p2ptoolbox.election.core.data.LeaseCommitUpdated;
import se.sics.p2ptoolbox.election.core.data.Promise;
import se.sics.p2ptoolbox.election.core.msg.LeaderExtensionRequest;
import se.sics.p2ptoolbox.election.core.msg.LeaderPromiseMessage;
import se.sics.p2ptoolbox.election.core.msg.LeaseCommitMessageUpdated;
import se.sics.p2ptoolbox.election.core.util.ElectionHelper;
import se.sics.p2ptoolbox.election.core.util.LeaderFilter;
import se.sics.p2ptoolbox.election.core.util.PromiseResponseTracker;
import se.sics.p2ptoolbox.election.core.util.TimeoutCollection;
import se.sics.p2ptoolbox.gradient.api.GradientPort;
import se.sics.p2ptoolbox.gradient.api.msg.GradientSample;

import java.security.PublicKey;
import java.util.*;
import java.util.UUID;

/**
 * Leader Election Component.
 * <p/>
 * This is the core component which is responsible for election and the maintenance of the leader in the system.
 * The nodes are constantly analyzing the samples from the sampling service and based on the convergence tries to assert themselves
 * as the leader.
 * <p/>
 * <br/><br/>
 * <p/>
 * In addition to this, this component works on leases in which the leader generates a lease
 * and adds could happen only for that lease. The leader,  when the lease is on the verge of expiring tries to renew the lease by sending a special message to the
 * nodes in its view.
 * <p/>
 * <br/><br/>
 * <p/>
 * <b>NOTE: </b> The lease needs to be short enough so that if the leader dies, the system is not in a transitive state.
 * <p/>
 * <b>CAUTION: </b> Under development, so unstable to use as it is.
 */
public class ElectionLeader extends ComponentDefinition {

    Logger logger = LoggerFactory.getLogger(ElectionLeader.class);

    private LCPeerView selfLCView;
    private LEContainer selfLEContainer;
    private ElectionConfig config;
    private long seed;
    private VodAddress selfAddress;
    private Map<VodAddress, LEContainer> addressContainerMap;
    private LeaderFilter filter;

    // Promise Sub Protocol.
    private UUID electionRoundId;
    private TimeoutId promisePhaseTimeout;
    private TimeoutId leaseCommitPhaseTimeout;
    private PromiseResponseTracker electionRoundTracker;
    private PublicKey publicKey;

    private TimeoutId leaseTimeoutId;

    // Convergence Variables.
    int convergenceCounter;
    boolean isConverged;
    boolean inElection = false;
    boolean applicationAck = false;

    // LE Container View.
    private SortedSet<LEContainer> higherUtilityNodes;
    private SortedSet<LEContainer> lowerUtilityNodes;

    private Comparator<LCPeerView> lcPeerViewComparator;
    private Comparator<LEContainer> leContainerComparator;

    // Ports.
    Positive<Timer> timerPositive = requires(Timer.class);
    Positive<VodNetwork> networkPositive = requires(VodNetwork.class);
    Negative<LeaderElectionPort> electionPort = provides(LeaderElectionPort.class);
    Positive<GradientPort> gradientPort = requires(GradientPort.class);
    Negative<TestPort> testPortNegative = provides(TestPort.class);

    public ElectionLeader(ElectionInit<ElectionLeader> init) {

        doInit(init);
        subscribe(startHandler, control);
        subscribe(gradientSampleHandler, gradientPort);
        subscribe(viewUpdateHandler, electionPort);

        // Test Sample
        subscribe(mockedUpdateHandler, testPortNegative);

        // Promise Subscriptions.
        subscribe(promiseResponseHandler, networkPositive);
        subscribe(promiseRoundTimeoutHandler, timerPositive);

        // Lease Subscriptions.
        subscribe(leaseTimeoutHandler, timerPositive);
        subscribe(leaseResponseHandler, networkPositive);
        subscribe(leaseResponseTimeoutHandler, timerPositive);
    }


    // Init method.
    private void doInit(ElectionInit<ElectionLeader> init) {

        this.config = init.electionConfig;
        this.seed = init.seed;
        this.selfAddress = init.selfAddress;
        this.filter = init.filter;
        this.publicKey = init.publicKey;

        // voting protocol.
        isConverged = false;
        electionRoundTracker = new PromiseResponseTracker();

        this.selfLCView = init.initialView;
        this.selfLEContainer = new LEContainer(selfAddress, selfLCView);
        this.addressContainerMap = new HashMap<VodAddress, LEContainer>();


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
            logger.trace("{}: Leader Election Component is up", selfAddress.getId());
        }
    };


    Handler<MockedGradientUpdate> mockedUpdateHandler = new Handler<MockedGradientUpdate>() {
        @Override
        public void handle(MockedGradientUpdate event) {

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
            Pair<SortedSet<LEContainer>, SortedSet<LEContainer>> lowerAndHigherViewPair = ElectionHelper.getHigherAndLowerViews(addressContainerMap.values(), leContainerComparator, selfLEContainer);
            lowerUtilityNodes = lowerAndHigherViewPair.getValue0();
            higherUtilityNodes = lowerAndHigherViewPair.getValue1();

            checkIfLeader();
        }
    };


    Handler<ViewUpdate> viewUpdateHandler = new Handler<ViewUpdate>() {
        @Override
        public void handle(ViewUpdate viewUpdate) {

            LCPeerView oldView = selfLCView;
            selfLCView = viewUpdate.selfPv;
            selfLEContainer = new LEContainer(selfAddress, selfLCView);

            logger.trace(" {}: Received view update from the application: {} ", selfAddress.getId(), selfLCView.toString());


            // This part of tricky to understand. The follower component works independent of the leader component.
            // In order to prevent the leader from successive tries while waiting on the update from the application regarding being in the group membership or not, currently
            // the node starts an election round with a unique id and in case it reaches the lease commit phase the outcome is not deterministic as the responses might be late or something.
            // So we reset the election round only when we receive an update from the application with the same roundid.
            //
            // ElectionLeader -> ElectionFollower -> Application : broadcast (ElectionFollower || ElectionLeader).

            // Got some view update from the application and I am currently in election.
            if (viewUpdate.electionRoundId != null && inElection) {
                
                if (electionRoundId != null && electionRoundId.equals(viewUpdate.electionRoundId)) {

                    if (electionRoundTracker.getRoundId() != null && electionRoundTracker.getRoundId().equals(viewUpdate.electionRoundId)) {
                        applicationAck = true;  // I am currently tracking the round and as application being fast I received the ack for the round from application.
                    } else{
                       resetElectionMetaData(); // Finally the application update has arrived and now I can let go of the election round.
                    }
                }
            }


            // The application using the protocol can direct it to forcefully terminate the leadership in case 
            // a specific event happens at the application end. The decision is implemented using the filter which the application injects in the
            // protocol during the booting up.
            
            if (filter.terminateLeader(oldView, selfLCView)) {
                
                CancelTimeout ct = new CancelTimeout(leaseTimeoutId);
                trigger(ct, timerPositive);
                leaseTimeoutId = null;

                terminateBeingLeader();

                // reset the convergence parameters for application to again achieve convergence.
                isConverged = false;
                convergenceCounter = 0;
            }
        }
    };

    /**
     * Handler for the gradient sample that we receive from the gradient in the system.
     * Incorporate the gradient sample to recalculate the convergence and update the view of higher or lower utility nodes.
     */
    Handler<GradientSample> gradientSampleHandler = new Handler<GradientSample>() {

        @Override
        public void handle(GradientSample event) {

            logger.trace("{}: Received sample from gradient", selfAddress.getId());

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

        // I don't see anybody above me, so should start voting.
        // Addition lease check is required because for the nodes which are in the node group will be acting under
        // lease of the leader, with special variable check.

        if (isConverged && higherUtilityNodes.size() == 0 && !inElection && !selfLCView.isLeaderGroupMember()) {
            if (addressContainerMap.size() < config.getViewSize()) {
                logger.warn(" {}: I think I am leader but the view less than the minimum requirement, so returning.", selfAddress.getId());
                return;
            }

            startVoting();
        }
    }


    /**
     * In case the node sees itself a candidate for being the leader,
     * it initiates voting.
     */
    private void startVoting() {

        logger.warn("{}: Starting with the voting .. ", selfAddress.getId());
        electionRoundId = UUID.randomUUID();
        applicationAck = false;
        
        Promise.Request request = new Promise.Request(selfAddress, selfLCView, electionRoundId);
        int leaderGroupSize = Math.min(Math.round((float)config.getViewSize() / 2), config.getMaxLeaderGroupSize());
        Collection<LEContainer> leaderGroupNodes = createLeaderGroupNodes(leaderGroupSize);


        if (leaderGroupNodes.size() < leaderGroupSize) {
            logger.error(" {} : Not asserting self as leader as the leader group size is less than required.", selfAddress.getId());
            return;
        }

        // Add SELF to the leader group nodes.
        leaderGroupNodes.add(new LEContainer(selfAddress, selfLCView));

        inElection = true;
        Collection<VodAddress> leaderGroupAddress = new ArrayList<VodAddress>();

        for (LEContainer leaderGroupNode : leaderGroupNodes) {

            VodAddress lgMemberAddr = leaderGroupNode.getSource();
            leaderGroupAddress.add(lgMemberAddr);
            
            logger.warn("Sending Promise Request to : " + lgMemberAddr.getId());
            LeaderPromiseMessage.Request promiseRequest = new LeaderPromiseMessage.Request(selfAddress, lgMemberAddr, electionRoundId, request);
            trigger(promiseRequest, networkPositive);
            leaderGroupSize--;
        }

        electionRoundTracker.startTracking(electionRoundId, leaderGroupAddress);

        ScheduleTimeout st = new ScheduleTimeout(5000);
        st.setTimeoutEvent(new TimeoutCollection.PromiseRoundTimeout(st));
        promisePhaseTimeout = st.getTimeoutEvent().getTimeoutId();

        trigger(st, timerPositive);
    }


    Handler<LeaderPromiseMessage.Response> promiseResponseHandler = new Handler<LeaderPromiseMessage.Response>() {
        @Override
        public void handle(LeaderPromiseMessage.Response event) {

            logger.debug("{}: Received Promise Response from : {} ", selfAddress.getId(), event.getSource().getId());
            int numPromises = electionRoundTracker.addPromiseResponseAndGetSize(event);

            if (numPromises >= electionRoundTracker.getLeaderGroupInformationSize()) {

                CancelTimeout cancelTimeout = new CancelTimeout(promisePhaseTimeout);
                trigger(cancelTimeout, timerPositive);
                promisePhaseTimeout = null;

                if (electionRoundTracker.isAccepted()) {

                    logger.warn("{}: All the leader group nodes have promised.", selfAddress.getId());
                    LeaseCommitUpdated.Request request = new LeaseCommitUpdated.Request(selfAddress,
                            publicKey, selfLCView, electionRoundTracker.getRoundId());

                    for (VodAddress address : electionRoundTracker.getLeaderGroupInformation()) {
                        logger.warn("Sending Commit Request to : " + address.getId());
                        trigger(new LeaseCommitMessageUpdated.Request(selfAddress,
                                address, UUID.randomUUID(), request), networkPositive);
                    }

                    ScheduleTimeout st = new ScheduleTimeout(5000);
                    st.setTimeoutEvent(new TimeoutCollection.LeaseCommitResponseTimeout(st));
                    leaseCommitPhaseTimeout = st.getTimeoutEvent().getTimeoutId();

                    trigger(st, timerPositive);

                } else {
                    inElection = false;
                    electionRoundTracker.resetTracker();
                }
            }
        }
    };


    /**
     * Handler for the response that the node receives as part of the lease commit phase. Aggregate the responses and check if every node has
     * committed.
     */
    Handler<LeaseCommitMessageUpdated.Response> leaseResponseHandler = new Handler<LeaseCommitMessageUpdated.Response>() {
        @Override
        public void handle(LeaseCommitMessageUpdated.Response event) {

            logger.warn("Received lease commit response from the node: {} , response: {}",event.getVodSource().getId() , event.content.isCommit);
            
            int commitResponses = electionRoundTracker.addLeaseCommitResponseAndgetSize(event.content);
            if (commitResponses >= electionRoundTracker.getLeaderGroupInformationSize()) {

                CancelTimeout cancelTimeout = new CancelTimeout(leaseCommitPhaseTimeout);
                trigger(cancelTimeout, timerPositive);
                leaseCommitPhaseTimeout = null;

                if (electionRoundTracker.isLeaseCommitAccepted()) {

                    logger.warn("{}: All the leader group nodes have committed the lease.", selfAddress.getId());
                    trigger(new LeaderState.ElectedAsLeader(electionRoundTracker.getLeaderGroupInformation()), electionPort);

                    ScheduleTimeout st = new ScheduleTimeout(config.getLeaderLeaseTime());
                    st.setTimeoutEvent(new TimeoutCollection.LeaseTimeout(st));

                    leaseTimeoutId = st.getTimeoutEvent().getTimeoutId();
                    trigger(st, timerPositive);

                    logger.warn("Setting self as leader complete.");
                }

                if(applicationAck){

                    applicationAck = false;
                    resetElectionMetaData();
                }
                
                // Seems my application component is kind of running late and therefore I still have not received 
                // ack from the application, even though the follower seems to have sent it to the application.
                electionRoundTracker.resetTracker();
            }
        }

    };


    /**
     * The round for getting the promises from the nodes in the system, timed out and therefore there is no need to wait for them.
     * Reset the round tracker variable and the election phase.
     */
    Handler<TimeoutCollection.PromiseRoundTimeout> promiseRoundTimeoutHandler = new Handler<TimeoutCollection.PromiseRoundTimeout>() {
        @Override
        public void handle(TimeoutCollection.PromiseRoundTimeout event) {

            if(promisePhaseTimeout != null && promisePhaseTimeout.equals(event.getTimeoutId())){
                logger.warn("{}: Election Round Timed Out in the promise phase.", selfAddress.getId());
                resetElectionMetaData();
                electionRoundTracker.resetTracker();
            }
            else{
                logger.warn("Promise already supposed to be fulfilled but timeout triggered");
            }

        }
    };


    private void resetElectionMetaData(){
        inElection = false;
        electionRoundId = null;
    }
    
    /**
     * Handler on the leader component indicating that node couldn't receive all the
     * commit responses associated with the lease were not received on time and therefore it has to reset the state information.
     */
    Handler<TimeoutCollection.LeaseCommitResponseTimeout> leaseResponseTimeoutHandler = new Handler<TimeoutCollection.LeaseCommitResponseTimeout>() {
        @Override
        public void handle(TimeoutCollection.LeaseCommitResponseTimeout event) {
            
            logger.warn("{}: Election Round timed out in the lease commit phase.", selfAddress.getId());
            if(leaseCommitPhaseTimeout != null && leaseCommitPhaseTimeout.equals(event.getTimeoutId())){

                electionRoundTracker.resetTracker();

                if(applicationAck){
                    
                    applicationAck = false;
                    resetElectionMetaData(); // Reset election phase if already received ack for the commit that I sent to local follower component.
                }
            }
            else{
                logger.warn("{}: Received the timeout after being cancelled.", selfAddress.getId());
            }

        }
    };

    /**
     * Lease for the current round timed out.
     * Now we need to reset some parameters in order to let other nodes to try and assert themselves as leader.
     */

    Handler<TimeoutCollection.LeaseTimeout> leaseTimeoutHandler = new Handler<TimeoutCollection.LeaseTimeout>() {
        @Override
        public void handle(TimeoutCollection.LeaseTimeout event) {

            logger.debug(" Special : Lease Timed out at Leader End: {} , trying to extend the lease", selfAddress.getId());
            if (isExtensionPossible()) {

                logger.warn("Trying to extend the leadership.");

                int leaderGroupSize = Math.min(config.getViewSize() / 2 + 1, config.getMaxLeaderGroupSize());
                Collection<LEContainer> lowerNodes = createLeaderGroupNodes(leaderGroupSize);

                if (lowerNodes.size() < leaderGroupSize) {

                    logger.error("{}: Terminate being the leader as state seems to be corrupted.", selfAddress.getId());
                    terminateBeingLeader();
                    return;
                }

                lowerNodes.add(new LEContainer(selfAddress, selfLCView));
                
                
                UUID roundId = UUID.randomUUID();
                
                for (LEContainer container : lowerNodes) {
                    trigger(new LeaderExtensionRequest(selfAddress, container.getSource(), roundId, new ExtensionRequest(selfAddress, publicKey, selfLCView, roundId)), networkPositive);
                }

                // Extend the lease.
                ScheduleTimeout st = new ScheduleTimeout(config.getLeaderLeaseTime());
                st.setTimeoutEvent(new TimeoutCollection.LeaseTimeout(st));
                trigger(st, timerPositive);

            } else {
                logger.warn("{}: Will Not extend the lease anymore.", selfAddress.getId());
                terminateBeingLeader();
            }
        }
    };


    /**
     * In case the leader sees some node above himself, then
     * keeping in mind the fairness policy the node should terminate.
     */
    private void terminateBeingLeader() {

        // Disable leadership and membership.
        resetElectionMetaData();
        trigger(new LeaderState.TerminateBeingLeader(), electionPort);
    }


    /**
     * Check if the leader is still suited to be the leader.
     *
     * @return extension possible
     */
    private boolean isExtensionPossible() {

        boolean extensionPossible = true;

        if (addressContainerMap.size() < config.getViewSize()) {
            extensionPossible = false;
        } else {

            SortedSet<LCPeerView> updatedSortedContainerSet = new TreeSet<LCPeerView>(lcPeerViewComparator);

            for (LEContainer container : addressContainerMap.values()) {
                updatedSortedContainerSet.add(container.getLCPeerView().disableLGMembership());
            }
            LCPeerView updatedTempSelfView = selfLCView.disableLGMembership();

            if (updatedSortedContainerSet.tailSet(updatedTempSelfView).size() != 0) {
                extensionPossible = false;
            }
        }

        return extensionPossible;
    }


    /**
     * Construct a collection of nodes which the leader thinks should be in the leader group
     *
     * @param size size of the leader group
     * @return Leader Group Collection.
     */
    private Collection<LEContainer> createLeaderGroupNodes(int size) {

        Collection<LEContainer> collection = new ArrayList<LEContainer>();

        if (size <= lowerUtilityNodes.size()) {

            Iterator<LEContainer> iterator = ((TreeSet) lowerUtilityNodes).descendingIterator();
            while (iterator.hasNext() && size > 0) {

                collection.add(iterator.next());
                size--;
            }
        }

        return collection;
    }

}
