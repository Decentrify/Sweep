package se.sics.p2ptoolbox.election.core;

import se.sics.gvod.net.VodAddress;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Init;
import se.sics.p2ptoolbox.election.api.LCPeerView;

/**
 * Base Init class for the core classes involved in leader election.
 * Created by babbar on 2015-04-04.
 */
public class ElectionInit<T extends ComponentDefinition> extends  Init <T> {

    public final VodAddress selfAddress;
    public final LCPeerView initialView;
    public final long seed;
    public final ElectionConfig electionConfig;

    public ElectionInit(VodAddress selfAddress, LCPeerView initialView, long seed, ElectionConfig electionConfig){
        this.selfAddress = selfAddress;
        this.seed = seed;
        this.initialView = initialView;
        this.electionConfig = electionConfig;
    }

}
