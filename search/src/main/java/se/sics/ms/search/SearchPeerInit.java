package se.sics.ms.search;

import se.sics.cm.ChunkManagerConfiguration;
import se.sics.gvod.common.Self;
import se.sics.gvod.config.ElectionConfiguration;
import se.sics.gvod.config.GradientConfiguration;
import se.sics.gvod.config.SearchConfiguration;
import se.sics.gvod.net.VodAddress;
import se.sics.kompics.Init;
import se.sics.p2ptoolbox.croupier.core.CroupierConfig;
import se.sics.p2ptoolbox.gradient.core.GradientConfig;

public final class SearchPeerInit extends Init<SearchPeer> {

    private final Self self;
    private final CroupierConfig croupierConfiguration;
    private final SearchConfiguration applicationConfiguration;
    private final GradientConfiguration pseudoGradientConfiguration;
    private final ElectionConfiguration electionConfiguration;
    private final ChunkManagerConfiguration chunkManagerConfiguration;
    private final GradientConfig gradientConfig;
    private final VodAddress bootstrappingNode;
    private final VodAddress simulatorAddress;

    public SearchPeerInit(Self self,
            CroupierConfig croupierConfiguration, SearchConfiguration applicationConfiguration,
            GradientConfiguration pseudoGradientConfiguration, ElectionConfiguration electionConfiguration,
            ChunkManagerConfiguration chunkManagerConfiguration, GradientConfig gradientConfig,
            VodAddress bootstrappingNode, VodAddress simulatorAddress) {
        super();
        this.self = self;
        this.croupierConfiguration = croupierConfiguration;
        this.applicationConfiguration = applicationConfiguration;
        this.pseudoGradientConfiguration = pseudoGradientConfiguration;
        this.electionConfiguration = electionConfiguration;
        this.chunkManagerConfiguration = chunkManagerConfiguration;
        this.gradientConfig  = gradientConfig;
        this.bootstrappingNode = bootstrappingNode;
        this.simulatorAddress = simulatorAddress;
    }

    public Self getSelf() {
        return this.self;
    }

    public CroupierConfig getCroupierConfiguration() {
        return this.croupierConfiguration;
    }

    public SearchConfiguration getSearchConfiguration() {
        return this.applicationConfiguration;
    }

    public GradientConfiguration getPseudoGradientConfiguration() {
        return this.pseudoGradientConfiguration;
    }

    public ElectionConfiguration getElectionConfiguration() {
        return this.electionConfiguration;
    }

    public VodAddress getBootstrappingNode() {
        return bootstrappingNode;
    }
    
    public GradientConfig getGradientConfig(){
        return this.gradientConfig;
    }
    
    public ChunkManagerConfiguration getChunkManagerConfiguration() {
        return chunkManagerConfiguration;
    }
    
    public VodAddress getSimulatorAddress(){
        return this.simulatorAddress;
    }
}
