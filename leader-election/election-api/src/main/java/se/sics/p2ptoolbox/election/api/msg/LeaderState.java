package se.sics.p2ptoolbox.election.api.msg;

import se.sics.kompics.KompicsEvent;


/**
 * Wrapper class for the marker events for node becoming or losing leader
 * status.
 */
public class LeaderState {
    
    public static class ElectedAsLeader implements KompicsEvent{
    }
    
    public static class TerminateBeingLeader implements KompicsEvent{
    }
    
}
