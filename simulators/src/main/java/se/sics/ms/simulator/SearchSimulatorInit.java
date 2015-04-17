package se.sics.ms.simulator;

import se.sics.cm.ChunkManagerConfiguration;
import se.sics.gvod.config.ElectionConfiguration;
import se.sics.gvod.config.GradientConfiguration;
import se.sics.gvod.config.SearchConfiguration;
import se.sics.kompics.Init;
import se.sics.p2ptoolbox.croupier.CroupierConfig;
import se.sics.p2ptoolbox.election.core.ElectionConfig;
import se.sics.p2ptoolbox.gradient.GradientConfig;

public final class SearchSimulatorInit extends Init<SearchSimulator> {
	private final CroupierConfig croupierConfiguration;
	private final GradientConfiguration gradientConfiguration;
	private final SearchConfiguration searchConfiguration;
	private final ElectionConfiguration electionConfiguration;
    private final ChunkManagerConfiguration chunkManagerConfiguration;
    private final ElectionConfig electionConfig;

    private final GradientConfig gradientConfig;
	public SearchSimulatorInit(CroupierConfig croupierConfiguration, GradientConfiguration gradientConfiguration,
                               SearchConfiguration aggregationConfiguration, ElectionConfiguration electionConfiguration,
                               ChunkManagerConfiguration chunkManagerConfiguration, GradientConfig gradientConfig, ElectionConfig electionConfig) {
		super();
		this.croupierConfiguration = croupierConfiguration;
		this.gradientConfiguration = gradientConfiguration;
		this.searchConfiguration = aggregationConfiguration;
		this.electionConfiguration = electionConfiguration;
        this.chunkManagerConfiguration = chunkManagerConfiguration;
        this.gradientConfig = gradientConfig;
        this.electionConfig = electionConfig;
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

    public GradientConfig getGradientConfig() {
        return gradientConfig;
    }

    public ElectionConfig getElectionConfig(){
        return this.electionConfig;
    }
}
