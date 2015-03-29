package se.sics.p2ptoolbox.election.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.address.Address;
import se.sics.gvod.croupier.Croupier;
import se.sics.gvod.net.VodAddress;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.timer.*;
import se.sics.gvod.timer.Timer;
import se.sics.kompics.*;
import se.sics.p2ptoolbox.croupier.api.util.CroupierPeerView;
import se.sics.p2ptoolbox.croupier.api.util.PeerView;
import se.sics.p2ptoolbox.election.api.ports.LeaderElectionPort;
import se.sics.p2ptoolbox.election.core.data.VotingRequest;
import se.sics.p2ptoolbox.election.core.data.VotingResponse;
import se.sics.p2ptoolbox.election.core.msg.VotingMessage;
import se.sics.p2ptoolbox.election.core.util.LeaderView;
import se.sics.p2ptoolbox.election.core.util.VotingResponseTracker;
import se.sics.p2ptoolbox.gradient.api.GradientPort;
import se.sics.p2ptoolbox.gradient.api.msg.GradientSample;

import java.security.PublicKey;
import java.util.*;
import java.util.UUID;

/**
 * Leader Election Component.
 *  
 * This is the core component which is responsible for election and the maintainence of the leader in the system.
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
 * Created by babbarshaer on 2015-03-27.
 */
public class LeaderElection extends ComponentDefinition{

    Logger logger = LoggerFactory.getLogger(LeaderElection.class);
    
    private PeerView selfPV;
    private ElectionConfig config;
    private long seed;
    private VodAddress selfAddress;
    private Map<VodAddress, PeerView> viewMap;
    
    
    // Leader Selection Variables.
    private boolean amILeader;
    private VodAddress leaderAddress;
    private PublicKey leaderPublicKey;
    private UUID voteId;
    private TimeoutId votingTimeout;
    private VotingResponseTracker responseTracker;
    
    
    // Convergence Variables.
    private Set<VodAddress> viewAddressSet;
    int convergenceCounter;
    boolean isConverged;
    boolean isUnderLease;
    
    private SortedSet<CroupierPeerView> totalOrderedNodes;
    private SortedSet<CroupierPeerView> higherUtilityNodes;
    private SortedSet<CroupierPeerView> lowerUtilityNodes;
    private Comparator<PeerView> pvUtilityComparator;
    private Comparator<CroupierPeerView> cpvUtilityComparator;
    
    // Ports.
    Positive<Timer> timerPositive = requires(Timer.class);
    Positive<VodNetwork> networkPositive = requires(VodNetwork.class);
    Negative<LeaderElectionPort> electionPort = provides(LeaderElectionPort.class);
    Positive<GradientPort> gradientPort = requires(GradientPort.class);
    
    public LeaderElection(LeaderElectionInit init){
        
        doInit(init);
        subscribe(startHandler, control);
        subscribe(gradientSampleHandler, gradientPort);
        subscribe(leaseTimeoutHandler, timerPositive);
        
        // Voting Subscriptions.
        subscribe(votingTimeoutHandler, timerPositive);
        subscribe(electionRequestHandler, networkPositive);
    }


    /**
     * Timeout representing when the lease expires.
     */
    public class LeaseTimeout extends Timeout{
        
        public LeaseTimeout(ScheduleTimeout request) {
            super(request);
        }
    }


    /**
     * Timeout representing when the voting expires.
     */
    public class VoteTimeout extends Timeout{
        
        public VoteTimeout(ScheduleTimeout request) {
            super(request);
        }
    }
    
    // Init method.
    private void doInit(LeaderElectionInit init) {
        this.selfPV = init.selfView;
        this.config = init.electionConfig;
        this.seed = init.seed;
        this.selfAddress = init.selfAddress;

        // voting protocol.
        isConverged = false;
        amILeader = false;
        responseTracker = new VotingResponseTracker();
        
        viewMap = new HashMap<VodAddress, PeerView>();
        pvUtilityComparator = config.getUtilityComparator();
        cpvUtilityComparator = new UtilityComparator();
       
        lowerUtilityNodes = new TreeSet<CroupierPeerView>(cpvUtilityComparator);
        higherUtilityNodes = new TreeSet<CroupierPeerView>(cpvUtilityComparator);
        
    }

    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            logger.debug("Started the Leader Election Component ...");
        }
    };
    
    
    Handler<GradientSample> gradientSampleHandler = new Handler<GradientSample>() {
        @Override
        public void handle(GradientSample event) {
            
            logger.debug("{}: Received sample from gradient", selfAddress.getId());
            
            incorporateSample(event.gradientSample, new CroupierPeerView(selfPV, selfAddress));
            checkIfLeader();
        }
    };
    
    /**
     * The node after incorporating the sample, checks if it
     * is in a position to assert itself as a leader.
     */
    private void checkIfLeader() {
        
        if(viewMap.size() < config.getViewSize()){
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

        voteId = UUID.randomUUID();
        VotingRequest request = new VotingRequest(selfPV);
        responseTracker.startTracking(voteId);
        
        for(VodAddress dest : viewMap.keySet()){
            VotingMessage.Request votingRequestMessage = new VotingMessage.Request(selfAddress,dest, voteId,request);
            trigger(votingRequestMessage, networkPositive);
        }

        ScheduleTimeout st = new ScheduleTimeout(config.getVoteTimeout());
        st.setTimeoutEvent(new VoteTimeout(st));
        votingTimeout = st.getTimeoutEvent().getTimeoutId();
        
        trigger(st, timerPositive);
    }
    
    
    Handler<VotingMessage.Request> electionRequestHandler = new Handler<VotingMessage.Request>() {
        @Override
        public void handle(VotingMessage.Request event) {
            
            logger.debug("Received voting request from : {}", event.getVodDestination().getId());
            VotingMessage.Response response;
            boolean acceptCandidate = true;

            if(isUnderLease){
                acceptCandidate = false;
            }
            
            else {
                
                PeerView highestPreset = getHighestUtilityNode();
                if(pvUtilityComparator.compare(highestPreset, event.content.leaderSelfView) > 0){
                    acceptCandidate = false;
                }
            }
            
            response = new VotingMessage.Response(selfAddress, event.getVodSource(), event.id, new VotingResponse(acceptCandidate, isConverged));
            trigger(response, networkPositive);
            
        }
    };
    
    
    Handler<VotingMessage.Response> votingMessageResponse = new Handler<VotingMessage.Response>() {
        @Override
        public void handle(VotingMessage.Response event) {
            
            logger.info("{} Received voting message response from : {}" , selfAddress.getId(), event.getVodSource().getId());
            
            if(responseTracker.addVotingResponseAndIncrement(event) == config.getViewSize()){
                
                logger.debug("{}: All of the responses are received. ", selfAddress.getId());
                
                if(responseTracker.isAccepted() &&  (responseTracker.getConvergenceCount() * config.getConvergenceTest() > config.getViewSize()/2) ){
                    logger.debug("All nodes replied yes and most of them are converged");   
                }
                
            }
        }
        
    };
    
    
    
    
    
    
    
    
    
    
    
    Handler<VoteTimeout> votingTimeoutHandler = new Handler<VoteTimeout>() {
        @Override
        public void handle(VoteTimeout event) {
            logger.debug("{}: Voting event timed out now.", selfAddress.getId());
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


    Handler<LeaseTimeout> leaseTimeoutHandler = new Handler<LeaseTimeout>() {
        @Override
        public void handle(LeaseTimeout event) {
            logger.debug("Lease for the current leader: {} timed out", leaderAddress );
            resetLeaderInformation();
        }
    };

    /**
     * In case the lease expires or if the leader dies 
     * reset the information regarding the details about the current  
     * leader.
     * <br/>
     * <b>CAUTION: </b> We don't have any failure detection mechanism, 
     * so unable to detect for now if the leader is dead. 
     * We detect it when the lease expires.
     */
    public void resetLeaderInformation(){
        leaderAddress = null;
        leaderPublicKey = null;
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
     * Initialization Class for the Leader Election Component.
     */
    public static class LeaderElectionInit extends Init<LeaderElection>{
        
        VodAddress selfAddress;
        PeerView selfView;
        long seed;
        ElectionConfig electionConfig;
        
        public LeaderElectionInit(VodAddress selfAddress, PeerView selfView, long seed, ElectionConfig electionConfig){
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
