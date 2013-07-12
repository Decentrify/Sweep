package common.simulation;

import se.sics.gvod.config.CroupierConfiguration;
import se.sics.kompics.Init;
import se.sics.kompics.p2p.bootstrap.BootstrapConfiguration;

import common.configuration.CyclonConfiguration;
import common.configuration.ElectionConfiguration;
import common.configuration.SearchConfiguration;
import common.configuration.TManConfiguration;

public final class SimulatorInit extends Init {
	private final BootstrapConfiguration bootstrapConfiguration;
	private final CroupierConfiguration croupierConfiguration;
	private final TManConfiguration tmanConfiguration;
	private final SearchConfiguration aggregationConfiguration;
	private final ElectionConfiguration electionConfiguration;

	public SimulatorInit(BootstrapConfiguration bootstrapConfiguration,
                         CroupierConfiguration croupierConfiguration, TManConfiguration tmanConfiguration,
			SearchConfiguration aggregationConfiguration, ElectionConfiguration electionConfiguration) {
		super();
		this.bootstrapConfiguration = bootstrapConfiguration;
		this.croupierConfiguration = croupierConfiguration;
		this.tmanConfiguration = tmanConfiguration;
		this.aggregationConfiguration = aggregationConfiguration;
		this.electionConfiguration = electionConfiguration;
	}

	public SearchConfiguration getAggregationConfiguration() {
		return aggregationConfiguration;
	}

	public BootstrapConfiguration getBootstrapConfiguration() {
		return this.bootstrapConfiguration;
	}

	public CroupierConfiguration getCroupierConfiguration() {
		return this.croupierConfiguration;
	}

	public TManConfiguration getTmanConfiguration() {
		return this.tmanConfiguration;
	}
	
	public ElectionConfiguration getElectionConfiguration() {
		return this.electionConfiguration;
	}
}
