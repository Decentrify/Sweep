package se.sics.p2ptoolbox.election.core.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.p2ptoolbox.election.core.data.VotingResponse;
import se.sics.p2ptoolbox.election.core.msg.VotingMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Keeps Track of the voting responses
 * that a node receives from the peers in the system.
 *
 * Created by babbarshaer on 2015-03-29.
 */
public class VotingResponseTracker {

    private UUID voteCounter;
    private boolean amIAccepted;
    private int convergenceCounter;
    private List<VotingResponse> votingResponses;
    
    private Logger logger = LoggerFactory.getLogger(VotingResponseTracker.class);

    public VotingResponseTracker(){
        
        amIAccepted = true;
        convergenceCounter = 0;
        votingResponses = new ArrayList<VotingResponse>();
    }

    /**
     * A new voting round has started, therefore the tracking of responses needs to be reset.
     * @param voteCounter voting round id.
     */
    public void startTracking(UUID voteCounter){

        this.amIAccepted = true;
        this.convergenceCounter=0;
        
        this.voteCounter = voteCounter;
        this.votingResponses.clear();
    }

    /**
     * Add the voting response and update the associated state with the 
     * voting round that is being tracked.
     *
     * @param response Voting response from peer.
     * @return Response Collection Size for round.
     */
    public int addVotingResponseAndIncrement (VotingMessage.Response response){
        
        if(voteCounter == null || !response.id.equals(voteCounter)){
            logger.warn("Received a response that is not currently tracked.");
            return votingResponses.size();
        }
        
        VotingResponse votingResponse = response.content;
        amIAccepted = amIAccepted && votingResponse.acceptCandidate;
        convergenceCounter = votingResponse.isConverged ? (convergenceCounter + 1)  : convergenceCounter;
        
        votingResponses.add(response.content);
        return votingResponses.size();
    }


    /**
     * Completely reset the state.
     *  
     */
    public void reset(){
        
        this.voteCounter = null;
        this.votingResponses.clear();
        this.convergenceCounter = 0;
        this.amIAccepted = false;
    }


    /**
     * When the class is tracking voting round, the method returns  
     * that is the node accepted as leader at that point.
     * <br/>
     * In case the tracking information has been reset for some reason, the method simply
     * returns <b>false</b> to prevent any violation of safety.
     *
     * @return Is node accepted as leader till that point.
     */
    public boolean isAccepted(){
        return amIAccepted;
    }


    /**
     * Iterate over the responses 
     * @return Nodes Converged Count.
     */
    public int getConvergenceCount(){
        return convergenceCounter;
    }
    
}
