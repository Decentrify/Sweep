package se.sics.ms.search;

import se.sics.gvod.config.GradientConfiguration;
import se.sics.gvod.config.SearchConfiguration;
import se.sics.kompics.Init;
import se.sics.ms.common.ApplicationSelf;
import se.sics.p2ptoolbox.chunkmanager.ChunkManagerConfig;
import se.sics.p2ptoolbox.croupier.CroupierConfig;
import se.sics.p2ptoolbox.election.core.ElectionConfig;
import se.sics.p2ptoolbox.gradient.GradientConfig;
import se.sics.p2ptoolbox.tgradient.TreeGradientConfig;
import se.sics.p2ptoolbox.util.config.SystemConfig;

public final class SearchPeerInit extends Init<SearchPeer> {

    private final SystemConfig systemConfig;
    private final CroupierConfig croupierConfiguration;
    private final SearchConfiguration applicationConfiguration;
    private final GradientConfiguration pseudoGradientConfiguration;
    private final ChunkManagerConfig chunkManagerConfiguration;
    private final GradientConfig gradientConfig;
    private final ElectionConfig electionConfig;
    private final TreeGradientConfig tGradientConfig;
    
    public SearchPeerInit( SystemConfig systemConfig,
            CroupierConfig croupierConfiguration, SearchConfiguration applicationConfiguration,
            GradientConfiguration pseudoGradientConfiguration,
            ChunkManagerConfig chunkManagerConfiguration, GradientConfig gradientConfig,
            ElectionConfig electionConfig, TreeGradientConfig tGradientConfig) {
        super();
        this.systemConfig = systemConfig;
        this.croupierConfiguration = croupierConfiguration;
        this.applicationConfiguration = applicationConfiguration;
        this.pseudoGradientConfiguration = pseudoGradientConfiguration;
        this.chunkManagerConfiguration = chunkManagerConfiguration;
        this.gradientConfig  = gradientConfig;
        this.electionConfig = electionConfig;
        this.tGradientConfig = tGradientConfig;
    }

    public TreeGradientConfig getTGradientConfig() {
        return tGradientConfig;
    }

    public SystemConfig getSystemConfig(){
        return this.systemConfig;
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

    public GradientConfig getGradientConfig(){
        return this.gradientConfig;
    }
    
    public ChunkManagerConfig getChunkManagerConfig() {
        return chunkManagerConfiguration;
    }

    public ElectionConfig getElectionConfig(){
        return this.electionConfig;
    }
}
