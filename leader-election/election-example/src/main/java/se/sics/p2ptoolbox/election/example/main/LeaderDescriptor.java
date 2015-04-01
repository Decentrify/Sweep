package se.sics.p2ptoolbox.election.example.main;

import se.sics.p2ptoolbox.croupier.api.util.PeerView;
import se.sics.p2ptoolbox.election.api.LCPeerView;

/**
 * Descriptor that which will be exchanged by the gradient and used in the leader election protocol.
 *
 * Created by babbar on 2015-04-01.
 */
public class LeaderDescriptor implements PeerView, LCPeerView, Comparable<LeaderDescriptor>{


    private int utility;
    private boolean membership;

    public LeaderDescriptor(int utility){
        this.utility = utility;
        this.membership = false;
    }

    public LeaderDescriptor(int utility, boolean membership){
        this.utility = utility;
        this.membership = membership;
    }

    @Override
    public LCPeerView enableLGMembership() {
        return new LeaderDescriptor(this.utility, true);
    }

    @Override
    public LCPeerView disableLGMembership() {
        return new LeaderDescriptor(this.utility, false);
    }

    @Override
    public LeaderDescriptor deepCopy() {
        return null;
    }

    public void setMembership(boolean membership){
        this.membership = membership;
    }

    @Override
    public int compareTo(LeaderDescriptor o) {

        if(o == null){
            throw new IllegalArgumentException("Value being compared is null");
        }

        if(this.membership){
            if(!o.membership){
                return 1;
            }
        }

        else if(o.membership){
            if(!this.membership){
                return -1;
            }
        }

        return Integer.valueOf(this.utility).compareTo(o.utility);
    }
}
