package se.sics.p2ptoolbox.election.api;

/**
 * Interface for the Leader Election Capable Peer View.
 * 
 * Created by babbarshaer on 2015-03-29.
 */
public interface LCPeerView {


    /**
     * Checks if the node is part of the leader group.
     * The contract is dependent upon the call to
     * {@link #enableLGMembership()} and {@link #disableLGMembership()}
     *
     * @return is node part of leader group.
     */
    public boolean isLeaderGroupMember();

    /**
     * Indicates the view is now a part
     * of Leader Group.
     */
    public LCPeerView enableLGMembership();


    /**
     * Indicates node has been removed from
     * the Leader Group.
     */
    public LCPeerView disableLGMembership();
    
}
