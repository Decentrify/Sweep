package se.sics.p2ptoolbox.election.core.data;

/**
 * Response of the voting request by the nodes in the system.
 * It informs whether nodes accept or reject the leader.
 *
 * Created by babbarshaer on 2015-03-28.
 */
public class VotingResponse {

    public final boolean acceptCandidate;
    public final boolean isConverged;
    
    public VotingResponse(boolean acceptCandidate, boolean isConverged){
        this.acceptCandidate = acceptCandidate;
        this.isConverged = isConverged;
    }
}
