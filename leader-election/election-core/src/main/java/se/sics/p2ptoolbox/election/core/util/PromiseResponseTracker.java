package se.sics.p2ptoolbox.election.core.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.net.VodAddress;
import se.sics.p2ptoolbox.election.core.data.Promise;
import se.sics.p2ptoolbox.election.core.msg.LeaderPromiseMessage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * Keeps Track of the promises responses
 * that a node receives from the peers in the system.
 *
 * Created by babbarshaer on 2015-03-29.
 */
public class PromiseResponseTracker {

    private UUID promiseRoundId;
    private boolean amIAccepted;
    private int convergenceCounter;
    
    private Logger logger = LoggerFactory.getLogger(PromiseResponseTracker.class);
    private List<VodAddress> leaderGroupInformation;
    private List<Promise.Response> promiseResponseCollection;
    
    public PromiseResponseTracker(){
        
        amIAccepted = true;
        convergenceCounter = 0;
        leaderGroupInformation = new ArrayList<VodAddress>();
        promiseResponseCollection = new ArrayList<Promise.Response>();
        
    }

    /**
     * A new promise round has started, therefore the tracking of responses needs to be reset.
     * @param promiseRoundCounter promise round id.
     */
    public void startTracking(UUID promiseRoundCounter, Collection<VodAddress> leaderGroupInformation){

        this.amIAccepted = true;
        this.convergenceCounter=0;
        
        this.promiseRoundId = promiseRoundCounter;
        this.leaderGroupInformation.clear();
        this.leaderGroupInformation.addAll(leaderGroupInformation);
    }

    /**
     * Add the voting response and update the associated state with the 
     * voting round that is being tracked.
     *
     * @param response Voting response from peer.
     */
    public int addResponseAndGetSize (LeaderPromiseMessage.Response response){
        
        if(promiseRoundId == null || !response.id.equals(promiseRoundId)){
            logger.warn("Received a response that is not currently tracked.");
        }

        Promise.Response promiseResponse = response.content;
        amIAccepted = amIAccepted && promiseResponse.acceptCandidate;
        convergenceCounter = promiseResponse.isConverged ? (convergenceCounter + 1)  : convergenceCounter;
        
        promiseResponseCollection.add(promiseResponse);
        
        return promiseResponseCollection.size();
    }

    /**
     * Size of the current leader group.
     * @return leader group size.
     */
    public int getLeaderGroupInformationSize(){
        
        int size = 0;
        if(this.leaderGroupInformation != null){
            size = leaderGroupInformation.size();
        }
        
        return size;
    }


    public UUID getRoundId(){
        return this.promiseRoundId;
    }

    /**
     * Completely reset the state.
     *  
     */
    public void resetTracker(){
        
        this.promiseRoundId = null;
        this.convergenceCounter = 0;
        this.amIAccepted = false;
        
        this.leaderGroupInformation.clear();
        this.promiseResponseCollection.clear();
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


    /**
     * Return the information about the group that the node
     * trying to assert itself as leader thinks should be the leader group.
     *
     * @return LeaderGroupInformation.
     */
    public List<VodAddress> getLeaderGroupInformation(){
        return this.leaderGroupInformation;
    }
    
}
