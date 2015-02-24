package se.sics.ms.simulator;

import se.sics.cm.ChunkManagerConfiguration;
import se.sics.gvod.config.CroupierConfiguration;
import se.sics.gvod.config.ElectionConfiguration;
import se.sics.gvod.config.GradientConfiguration;
import se.sics.gvod.config.SearchConfiguration;
import se.sics.gvod.net.VodAddress;
import se.sics.kompics.Init;
import se.sics.p2ptoolbox.croupier.core.CroupierConfig;

/**
 * Created by babbarshaer on 2015-02-04.
 * 
 * Initialization class for the P2pValidatorMain.
 */
public class P2pSimulatorInit extends Init<P2pSim>{

    private final VodAddress self;
    private final VodAddress simulatorAddress;

    private final CroupierConfig croupierConfiguration;
    private final GradientConfiguration gradientConfiguration;
    private final SearchConfiguration searchConfiguration;
    private final ElectionConfiguration electionConfiguration;
    private final ChunkManagerConfiguration chunkManagerConfiguration;



    public P2pSimulatorInit(VodAddress self, VodAddress simulatorAddress, CroupierConfig croupierConfiguration, GradientConfiguration gradientConfiguration,
                               SearchConfiguration aggregationConfiguration, ElectionConfiguration electionConfiguration,
                               ChunkManagerConfiguration chunkManagerConfiguration) {
        super();
        this.self = self;
        this.simulatorAddress = simulatorAddress;
        this.croupierConfiguration = croupierConfiguration;
        this.gradientConfiguration = gradientConfiguration;
        this.searchConfiguration = aggregationConfiguration;
        this.electionConfiguration = electionConfiguration;
        this.chunkManagerConfiguration = chunkManagerConfiguration;
    }

    public SearchConfiguration getSearchConfiguration() {
        return searchConfiguration;
    }

    public CroupierConfig getCroupierConfiguration() {
        return this.croupierConfiguration;
    }

    public GradientConfiguration getGradientConfiguration() {
        return this.gradientConfiguration;
    }

    public ElectionConfiguration getElectionConfiguration() {
        return this.electionConfiguration;
    }

    public ChunkManagerConfiguration getChunkManagerConfiguration() {
        return chunkManagerConfiguration;
    }

    public VodAddress getSelf(){
        return this.self;
    }
    
    public VodAddress getSimulatorAddress(){
        return this.simulatorAddress;
    }
}
