package se.sics.ms.search;

import se.sics.cm.ChunkManagerConfiguration;
import se.sics.gvod.common.Self;
import se.sics.gvod.config.ElectionConfiguration;
import se.sics.gvod.config.GradientConfiguration;
import se.sics.gvod.config.SearchConfiguration;
import se.sics.gvod.net.VodAddress;
import se.sics.kompics.Init;
import se.sics.p2ptoolbox.croupier.core.CroupierConfig;

public final class SearchPeerInit extends Init<SearchPeer> {

    private final Self self;
    private final CroupierConfig croupierConfiguration;
    private final SearchConfiguration applicationConfiguration;
    private final GradientConfiguration gradientConfiguration;
    private final ElectionConfiguration electionConfiguration;
    private final ChunkManagerConfiguration chunkManagerConfiguration;
    private final VodAddress bootstrappingNode;
    private final VodAddress simulatorAddress;

    public SearchPeerInit(Self self,
            CroupierConfig croupierConfiguration, SearchConfiguration applicationConfiguration,
            GradientConfiguration gradientConfiguration, ElectionConfiguration electionConfiguration,
            ChunkManagerConfiguration chunkManagerConfiguration,
            VodAddress bootstrappingNode, VodAddress simulatorAddress) {
        super();
        this.self = self;
        this.croupierConfiguration = croupierConfiguration;
        this.applicationConfiguration = applicationConfiguration;
        this.gradientConfiguration = gradientConfiguration;
        this.electionConfiguration = electionConfiguration;
        this.chunkManagerConfiguration = chunkManagerConfiguration;
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

    public GradientConfiguration getGradientConfiguration() {
        return this.gradientConfiguration;
    }

    public ElectionConfiguration getElectionConfiguration() {
        return this.electionConfiguration;
    }

    public VodAddress getBootstrappingNode() {
        return bootstrappingNode;
    }

    public ChunkManagerConfiguration getChunkManagerConfiguration() {
        return chunkManagerConfiguration;
    }
    
    public VodAddress getSimulatorAddress(){
        return this.simulatorAddress;
    }
}
