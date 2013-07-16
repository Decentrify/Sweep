package se.sics.ms.simulator;

import se.sics.gvod.config.CroupierConfiguration;
import se.sics.gvod.config.ElectionConfiguration;
import se.sics.gvod.config.GradientConfiguration;
import se.sics.gvod.config.SearchConfiguration;
import se.sics.kompics.Init;

public final class SimulatorInit extends Init {
	private final CroupierConfiguration croupierConfiguration;
	private final GradientConfiguration gradientConfiguration;
	private final SearchConfiguration searchConfiguration;
	private final ElectionConfiguration electionConfiguration;

	public SimulatorInit(CroupierConfiguration croupierConfiguration, GradientConfiguration tmanConfiguration,
			SearchConfiguration aggregationConfiguration, ElectionConfiguration electionConfiguration) {
		super();
		this.croupierConfiguration = croupierConfiguration;
		this.gradientConfiguration = tmanConfiguration;
		this.searchConfiguration = aggregationConfiguration;
		this.electionConfiguration = electionConfiguration;
	}

	public SearchConfiguration getSearchConfiguration() {
		return searchConfiguration;
	}

	public CroupierConfiguration getCroupierConfiguration() {
		return this.croupierConfiguration;
	}

	public GradientConfiguration getGradientConfiguration() {
		return this.gradientConfiguration;
	}
	
	public ElectionConfiguration getElectionConfiguration() {
		return this.electionConfiguration;
	}
}
