package se.sics.ms.search;

import se.sics.ms.gvod.config.GradientConfiguration;
import se.sics.ms.gvod.config.SearchConfiguration;
import se.sics.kompics.Init;
import se.sics.ktoolbox.election.ElectionConfig;
import se.sics.ktoolbox.util.network.KAddress;

public final class SearchPeerInit extends Init<SearchPeer> {

    public final KAddress self;
    public final SearchConfiguration searchConfig;
    public final ElectionConfig electionConfig;
    public final GradientConfiguration gradientConfig;
    
    public SearchPeerInit(KAddress self, SearchConfiguration searchConfig, ElectionConfig electionConfig,
            GradientConfiguration gradientConfig) {
        this.self = self;
        this.searchConfig = searchConfig;
        this.electionConfig = electionConfig;
        this.gradientConfig = gradientConfig;
    }
}
