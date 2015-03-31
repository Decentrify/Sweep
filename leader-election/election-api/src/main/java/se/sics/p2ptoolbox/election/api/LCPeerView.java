package se.sics.p2ptoolbox.election.api;

/**
 * Interface for the Leader Election Capable Peer View.
 * 
 * Created by babbarshaer on 2015-03-29.
 */
public interface LCPeerView {


    /**
     * Indicates the view is now a part
     * of Leader Group.
     */
    public void enableLGMembership();


    /**
     * Indicates node has been removed from
     * the Leader Group.
     */
    public void disableLGMembership();
    
}
