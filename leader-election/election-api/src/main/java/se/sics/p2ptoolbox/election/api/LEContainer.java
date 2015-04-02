package se.sics.p2ptoolbox.election.api;

import se.sics.gvod.net.VodAddress;

/**
 * Container for the Leader Election Capable View.
 *
 * Created by babbar on 2015-03-31.
 */
public class LEContainer {

    VodAddress source;
    LCPeerView lcp;


    public LEContainer(VodAddress source, LCPeerView lcp){
        this.source = source;
        this.lcp = lcp;
    }

    public VodAddress getSource(){
        return this.source;
    }

    public LCPeerView getLCPeerView(){
        return this.lcp;
    }

    @Override
    public String toString() {
        return "LEContainer{" +
                "source=" + source +
                ", lcp=" + lcp +
                '}';
    }
}
