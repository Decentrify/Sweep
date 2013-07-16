package se.sics.ms.simulator;

import se.sics.gvod.config.CroupierConfiguration;
import se.sics.kompics.Init;

import se.sics.ms.configuration.ElectionConfiguration;
import se.sics.ms.configuration.SearchConfiguration;
import se.sics.ms.configuration.GradientConfiguration;

public final class SimulatorInit extends Init {
	private final CroupierConfiguration croupierConfiguration;
	private final GradientConfiguration tmanConfiguration;
	private final SearchConfiguration aggregationConfiguration;
	private final ElectionConfiguration electionConfiguration;

	public SimulatorInit(CroupierConfiguration croupierConfiguration, GradientConfiguration tmanConfiguration,
			SearchConfiguration aggregationConfiguration, ElectionConfiguration electionConfiguration) {
		super();
		this.croupierConfiguration = croupierConfiguration;
		this.tmanConfiguration = tmanConfiguration;
		this.aggregationConfiguration = aggregationConfiguration;
		this.electionConfiguration = electionConfiguration;
	}

	public SearchConfiguration getAggregationConfiguration() {
		return aggregationConfiguration;
	}

	public CroupierConfiguration getCroupierConfiguration() {
		return this.croupierConfiguration;
	}

	public GradientConfiguration getGradientConfiguration() {
		return this.tmanConfiguration;
	}
	
	public ElectionConfiguration getElectionConfiguration() {
		return this.electionConfiguration;
	}
}
