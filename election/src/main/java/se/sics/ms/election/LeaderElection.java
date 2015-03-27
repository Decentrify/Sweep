package se.sics.ms.election;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.config.ElectionConfiguration;
import se.sics.gvod.net.VodNetwork;
import se.sics.gvod.timer.*;
import se.sics.kompics.*;
import se.sics.ms.aggregator.core.StatusAggregator;
import se.sics.ms.aggregator.port.StatusAggregatorPort;
import se.sics.ms.common.MsSelfImpl;
import se.sics.ms.gradient.misc.UtilityComparator;
import se.sics.ms.gradient.ports.GradientViewChangePort;
import se.sics.ms.messages.ElectionMessage;
import se.sics.ms.messages.LeaderViewMessage;
import se.sics.ms.snapshot.Snapshot;
import se.sics.ms.timeout.IndividualTimeout;
import se.sics.ms.types.SearchDescriptor;

import java.security.PublicKey;
import java.sql.Time;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * This component will deal with the leader election.
 * In essence this component will keep looking at the nodes provided by the gradient and make
 * and informed decision regarding when to assert themselves as leaders.
 * <br/>
 * <p/>
 * The other half of the component will keep track of the current leader in terms of it's address and other
 * important information.
 * Created by babbarshaer on 2015-03-26.
 */
public class LeaderElection extends ComponentDefinition {

    // Local Variables
    private Logger logger = LoggerFactory.getLogger(LeaderElection.class);
    private ElectionConfiguration config;
    private MsSelfImpl self;
    private boolean iAmLeader, electionInProgress;
    private SortedSet<SearchDescriptor> higherUtilityNodes, lowerUtilityNodes;
    private int defaultComponentId;
    private TimeoutId voteTimeoutId;
    private PublicKey selfPublicKey = null;

    private UtilityComparator utilityComparator = new UtilityComparator();
    private boolean isConverged;
    private SearchDescriptor selfDescriptor;
    
    // Variables associated with voting.
    private int numberOfNodesAtVotingTime, electionCounter;
    private int totalVotes, yesVotes, convergedNodesCounter;

    // Ports.
    Positive<Timer> timerPositive = requires(Timer.class);
    Positive<VodNetwork> vodNetworkPositive = requires(VodNetwork.class);
    Positive<StatusAggregatorPort> statusAggregatorPortPositive = requires(StatusAggregatorPort.class);
    Negative<GradientViewChangePort> gradientViewChagedPort = provides(GradientViewChangePort.class);


    public LeaderElection(ElectionInit<LeaderElection> electionInit) {

        doInit(electionInit);
        subscribe(startHandler, control);
        subscribe(handleGradientBroadcast, gradientViewChagedPort);
        subscribe(voteTimeoutHandler, timerPositive);

        subscribe(votingRequestHandler, vodNetworkPositive);
        subscribe(votingResponseHandler, vodNetworkPositive);
    }


    /**
     * Initialization Tasks.
     *
     * @param init init
     */
    private void doInit(ElectionInit<LeaderElection> init) {

        self = (MsSelfImpl) init.getSelf();
        config = init.getConfig();
        iAmLeader = false;
        electionInProgress = false;

        higherUtilityNodes = new TreeSet<SearchDescriptor>();
        lowerUtilityNodes = new TreeSet<SearchDescriptor>();


    }

    public class VoteTimeout extends Timeout {
        public VoteTimeout(ScheduleTimeout request) {
            super(request);
        }
    }


    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            logger.debug("Leader Election component started");
        }
    };



    /**
     * Handler for the periodic Gradient views that are being sent. It checks if the
     * node fulfills the requirements in order to become a leader, and in that
     * case it will call for a leader election.
     * <br/> <br/>
     * In addition to this, if the node is a leader, it checks if there is any new node higher than him.
     */
    final Handler<GradientViewChangePort.GradientViewChanged> handleGradientBroadcast = new Handler<GradientViewChangePort.GradientViewChanged>() {
        @Override
        public void handle(GradientViewChangePort.GradientViewChanged event) {

            //create a copy so other component  receiving the same copy of the object is not effected.
            SearchDescriptor currentDesc = new SearchDescriptor(self.getDescriptor());

            // We ran into some weird problems by creating a new set from the view of a tree set. Do not try that again.
            // Use the following approach in which you create copy of the whole set and then calculate the lower and higher utilities.
            SortedSet<SearchDescriptor> gradientSet = new TreeSet<SearchDescriptor>(event.getGradientView());

            higherUtilityNodes = gradientSet.tailSet(currentDesc);
            lowerUtilityNodes = gradientSet.headSet(currentDesc);
            isConverged = event.isConverged();

            if (isConverged
                    && !iAmLeader
                    && !electionInProgress
                    && higherUtilityNodes.size() == 0
                    && lowerUtilityNodes.size() >= config.getMinSizeOfElectionGroup()) {
                logger.debug("{}: Going to start voting to be a leader", self.getId());
                startVote();
            } else if (iAmLeader && higherUtilityNodes.size() != 0) {
                logger.warn("{}: I am leader and got a node higher than me in utility.", self.getId());
                stopBeingLeader();
            }
        }
    };


    /**
     * The gradient seems to be stabilized and the node sees that there is
     * no new node above it, so tries to become the leader by voting.
     * <br/>
     * Quorum approach will be used when checking the responses.
     */
    private void startVote() {

        electionInProgress = true;
        numberOfNodesAtVotingTime = lowerUtilityNodes.size();
        electionCounter++;

        ScheduleTimeout st = new ScheduleTimeout(config.getVoteRequestTimeout());
        st.setTimeoutEvent(new VoteTimeout(st));
        trigger(st, timerPositive);
        voteTimeoutId = st.getTimeoutEvent().getTimeoutId();

        ElectionMessage.Request vote;

        for (SearchDescriptor desc : lowerUtilityNodes) {
            vote = new ElectionMessage.Request(self.getAddress(), desc.getVodAddress(), voteTimeoutId,
                    electionCounter, new SearchDescriptor(self.getDescriptor()));
            trigger(vote, vodNetworkPositive);
        }

        trigger(st, timerPositive);
    }


    /**
     * Received message for the election request.
     */
    Handler<ElectionMessage.Request> votingRequestHandler = new Handler<ElectionMessage.Request>() {
        @Override
        public void handle(ElectionMessage.Request event) {
            logger.debug("{}: Received election request from: {}", self.getId(), event.getSource().getId());

            boolean acceptCandidate = true;
            SearchDescriptor highestUtilityNode = event.getLeaderCandidateDescriptor();

            if (iAmLeader) {
                // This a controversial check. It simply signifies that I give up my leadership quickly,
                // when I see a node above me from the gradient.
                acceptCandidate = false;
            } 
            else {
                SearchDescriptor descriptor = getHighestUtilityNode();
                
                if(utilityComparator.compare(descriptor, event.getLeaderCandidateDescriptor()) > 0){
                    acceptCandidate = false;
                    highestUtilityNode = descriptor;
                }
            }

            ElectionMessage.Response response = new ElectionMessage.Response(
                    self.getAddress(),
                    event.getVodSource(),
                    event.getTimeoutId(),
                    event.getVoteID(),
                    isConverged,
                    acceptCandidate,
                    highestUtilityNode);

            trigger(response, vodNetworkPositive);
        }
    };


    /**
     * Look at the highest utility node set and compare with the identifier received.
     *
     * @return Highest Utility Node
     */
    private SearchDescriptor getHighestUtilityNode() {

        SearchDescriptor highestDescriptorKnown;

        if (higherUtilityNodes.size() != 0) {
            highestDescriptorKnown = higherUtilityNodes.last();
        } else {
            highestDescriptorKnown = new SearchDescriptor(self.getDescriptor());
        }

        return highestDescriptorKnown;
    }


    /**
     * Handle the voting responses from the nodes in the gradient.
     */
    Handler<ElectionMessage.Response> votingResponseHandler = new Handler<ElectionMessage.Response>() {
        @Override
        public void handle(ElectionMessage.Response event) {
            
            logger.debug("{}: Received Election response from :{}, for election counter:{}", new Object[]{self.getId(), event.getVodSource(), event.getVoteId()});
            if(electionCounter == event.getVoteId() && electionInProgress){
                
                // Handle responses for current round only.
                totalVotes++;
                
                if(event.isVote()){
                    yesVotes++;
                }
                
                if(event.isConvereged()){
                    convergedNodesCounter++;
                }
                
                if(totalVotes >= numberOfNodesAtVotingTime){
                    
                    CancelTimeout cancelTimeout = new CancelTimeout(voteTimeoutId);
                    trigger(cancelTimeout, timerPositive);
                    
                    evaluateVotes();
                }
            }
        }
    };
    
    
    


    /**
     * Evaluate the votes, once the voting is complete
     * or the voting timed out. 
     */
    private void evaluateVotes() {

        logger.debug("Going to start with the evaluation of votes");
        
        // If all the returned votes are yes votes AND
        // there are nodes above the leader candidate in the tree AND
        // there are at least a certain number of nodes in the view AND
        // most of the voters are converged AND
        // they are above a certain ratio of the total number of nodes,
        // then the leader candidate will be elected leader

        if (yesVotes == totalVotes
                && higherUtilityNodes.size() == 0
                && lowerUtilityNodes.size() >= config.getMinSizeOfElectionGroup()
                && convergedNodesCounter >= config.getMinNumberOfConvergedNodes()
                && ((float) yesVotes >= Math.ceil((float) lowerUtilityNodes.size() * config.getMinPercentageOfVotes()))) {
            
            logger.debug("{}: Going to elect my self as a leader now.", self.getId());
            
            iAmLeader = true;
            broadcastInNetwork();
            broadcastLocally();
            
        }
        
        else{
            stopBeingLeader();
        }
    }


    /**
     * Send this information over the local port to the 
     * components listening for updates from the leader module.
     */
    private void broadcastLocally() {
        throw new UnsupportedOperationException("Operation not yet supported.");
    }


    /**
     * This method needs to be called when the node becomes the leader
     * and needs to disseminate this information in the gradient to other users.
     */
    private void broadcastInNetwork(){
        
        logger.debug("{}: Going to broadcast self view to the users.");
        for(SearchDescriptor descriptor : lowerUtilityNodes){
            LeaderViewMessage message = new LeaderViewMessage(self.getAddress(), descriptor.getVodAddress(), selfDescriptor, lowerUtilityNodes, selfPublicKey);
            trigger(message, vodNetworkPositive);
        }
    }
    

    /**
     * Voting timed out, evaluate the responses received.
     */
    Handler<VoteTimeout> voteTimeoutHandler = new Handler<VoteTimeout>() {
        @Override
        public void handle(VoteTimeout event) {
            logger.debug("Voting timed out, time to look at responses");
            evaluateVotes();
        }
    };


    /**
     * In case the node identifies any other node with higher utility,
     * it immediately stops being the leader.
     */
    private void stopBeingLeader() {
        
    }
}
