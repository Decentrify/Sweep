package se.sics.p2ptoolbox.election.core;

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
import se.sics.p2ptoolbox.election.core.data.Promise;
import se.sics.p2ptoolbox.election.core.msg.LeaderPromiseMessage;
import se.sics.p2ptoolbox.election.core.msg.LeaseCommitMessage;
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
    PeerView selfPV;

    private ElectionConfig config;
    private SortedSet<CroupierPeerView> higherUtilityNodes;
    private SortedSet<CroupierPeerView> lowerUtilityNodes;

    private Comparator<PeerView> pvUtilityComparator;
    private Comparator<CroupierPeerView> cpvUtilityComparator;

    // Gradient Sample.
    Set<VodAddress> viewAddressSet;
    int convergenceCounter =0;
    private boolean isConverged;
    
    // Leader Election.
    private UUID electionRoundId;
    private boolean isUnderLease;
    private TimeoutId awaitLeaseCommitId;
    private TimeoutId leaseTimeoutId;
    
    // Ports.
    Positive<VodNetwork> networkPositive = requires(VodNetwork.class);
    Positive<Timer> timerPositive = requires(Timer.class);
    Positive<GradientPort> gradientPort = requires(GradientPort.class);
    
    public ElectionFollower(ElectionFollowerInit init){
        
        doInit(init);
        subscribe(startHandler, control);
        subscribe(gradientSampleHandler, gradientPort);
        
        subscribe(promiseRequestHandler, networkPositive);
        subscribe(awaitLeaseCommitTimeoutHandler, timerPositive);
        subscribe(leaseCommitRequestHandler, networkPositive);
        subscribe(leaseTimeoutHandler, networkPositive);
    }

    private void doInit(ElectionFollowerInit init) {

        config = init.electionConfig;
        selfAddress = init.selfAddress;
        selfPV = init.selfView;

        viewAddressSet = new HashSet<VodAddress>();
        
        pvUtilityComparator = init.electionConfig.getUtilityComparator();
        cpvUtilityComparator = new UtilityComparator();
        
        lowerUtilityNodes = new TreeSet<CroupierPeerView>(cpvUtilityComparator);
        higherUtilityNodes = new TreeSet<CroupierPeerView>(cpvUtilityComparator);
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
            
            CroupierPeerView selfCPV = new CroupierPeerView(selfPV, selfAddress);       // TODO: Calculate the CPV, when you update the PV.
            incorporateSample(event.gradientSample, selfCPV);
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
            PeerView requestLeaderView = event.content.leaderView;

            boolean acceptCandidate = true;

            if(isUnderLease || (electionRoundId != null)) {
                // If part of leader group or already promised by setting election round id, I deny promise.
                acceptCandidate = false;
            }
            
            else{
                
                PeerView nodeToCompareTo = getHighestUtilityNode();
                if(pvUtilityComparator.compare(requestLeaderView, nodeToCompareTo) >= 0){

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
     * In order to incorporate the sample, first check if the convergence is reached.
     * After that, recalculate the lower and higher utility nodes based on the current
     * self view.
     *
     * @param cpvCollection collection
     * @param selfCPV self view
     */
    private void incorporateSample(Collection<CroupierPeerView> cpvCollection, CroupierPeerView selfCPV){

        // check for convergence of the nodes in system.
        updateViewAndConvergence(cpvCollection);

        // update the lower and higher utility nodes.
        SortedSet<CroupierPeerView> cpvSet = new TreeSet<CroupierPeerView>(cpvUtilityComparator);
        cpvSet.addAll(cpvCollection);

        lowerUtilityNodes = cpvSet.headSet(selfCPV);
        higherUtilityNodes = cpvSet.tailSet(selfCPV);
    }


    /**
     * Based on the collection provided, update the view held by the component
     * and also check for the convergence, which is decided by how much the view changed.
     *
     * @param cpvCollection view collection
     */
    private void updateViewAndConvergence(Collection<CroupierPeerView> cpvCollection){

        Set<VodAddress> oldAddressSet = new HashSet<VodAddress>(viewAddressSet);
        viewAddressSet.clear();

        for(CroupierPeerView cpv : cpvCollection){
            viewAddressSet.add(cpv.src);
        }

        int oldAddressSetSize = oldAddressSet.size();
        int newAddressSetSize = viewAddressSet.size();

        oldAddressSet.retainAll(viewAddressSet);

        if( (oldAddressSetSize == newAddressSetSize) &&  (oldAddressSet.size() > config.getConvergenceTest() * newAddressSetSize)) {

            convergenceCounter++;
            if(convergenceCounter > config.getConvergenceRounds()){
                isConverged =  true;
            }
        }

        else{

            // Detected some instability in system. So restart the convergence.
            convergenceCounter = 0;
            if(isConverged){
                isConverged = false;
            }
        }
    }
    
    
    
    
    
    
    
    /**
     * Analyze the views present locally and
     * return the highest utility node.
     *
     * @return Highest utility Node.
     */
    public PeerView getHighestUtilityNode(){

        if(higherUtilityNodes.size() != 0){
            return higherUtilityNodes.last().pv;
        }
        
        return selfPV;
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
        PeerView selfView;
        long seed;
        ElectionConfig electionConfig;

        public ElectionFollowerInit(VodAddress selfAddress, PeerView selfView, long seed, ElectionConfig electionConfig){
            this.selfAddress = selfAddress;
            this.selfView = selfView;
            this.seed = seed;
            this.electionConfig = electionConfig;
        }
    }


    private class UtilityComparator implements Comparator<CroupierPeerView> {

        @Override
        public int compare(CroupierPeerView o1, CroupierPeerView o2) {

            double compareToValue = Math.signum(pvUtilityComparator.compare(o1.pv, o2.pv));
            if (compareToValue == 0) {
                //should use CroupierPeerView compareTo to be equal consistent
                return (int) compareToValue;
            }
            return (int) compareToValue;
        }
    }
    
}
