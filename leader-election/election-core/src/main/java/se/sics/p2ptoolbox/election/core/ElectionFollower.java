package se.sics.p2ptoolbox.election.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.gvod.net.VodAddress;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.Start;
import se.sics.p2ptoolbox.croupier.api.util.CroupierPeerView;
import se.sics.p2ptoolbox.croupier.api.util.PeerView;

import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Election Follower part of the Leader Election Protocol.
 * 
 */
public class ElectionFollower extends ComponentDefinition{

    Logger logger = LoggerFactory.getLogger(ElectionFollower.class);
    
    VodAddress selfAddress;
    PeerView selfView;

    private ElectionConfig config;
    private SortedSet<CroupierPeerView> higherUtilityNodes;
    private SortedSet<CroupierPeerView> lowerUtilityNodes;

    private Comparator<PeerView> pvUtilityComparator;
    private Comparator<CroupierPeerView> cpvUtilityComparator;
    
    public ElectionFollower(ElectionFollowerInit init){
        doInit(init);
        subscribe(startHandler, control);
    }

    private void doInit(ElectionFollowerInit init) {

        config = init.electionConfig;
        selfAddress = init.selfAddress;
        selfView = init.selfView;
        
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
