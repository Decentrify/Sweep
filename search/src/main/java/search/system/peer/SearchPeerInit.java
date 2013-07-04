package search.system.peer;

import se.sics.gvod.common.Self;
import se.sics.gvod.config.CroupierConfiguration;
import se.sics.kompics.Init;
import se.sics.kompics.address.Address;
import se.sics.kompics.p2p.bootstrap.BootstrapConfiguration;

import common.configuration.CyclonConfiguration;
import common.configuration.ElectionConfiguration;
import common.configuration.SearchConfiguration;
import common.configuration.TManConfiguration;

public final class SearchPeerInit extends Init {
    private final Self self;
	private final BootstrapConfiguration bootstrapConfiguration;
	private final CroupierConfiguration croupierConfiguration;
	private final SearchConfiguration applicationConfiguration;
	private final TManConfiguration tManConfiguration;
	private final ElectionConfiguration electionConfiguration;

	public SearchPeerInit(Self self, BootstrapConfiguration bootstrapConfiguration,
                          CroupierConfiguration croupierConfiguration, SearchConfiguration applicationConfiguration,
			TManConfiguration tManConfiguration, ElectionConfiguration electionConfiguration) {
		super();
		this.self = self;
		this.bootstrapConfiguration = bootstrapConfiguration;
		this.croupierConfiguration = croupierConfiguration;
		this.applicationConfiguration = applicationConfiguration;
		this.tManConfiguration = tManConfiguration;
		this.electionConfiguration = electionConfiguration;
	}

	public Self getSelf() {
		return this.self;
	}

	public BootstrapConfiguration getBootstrapConfiguration() {
		return this.bootstrapConfiguration;
	}

	public CroupierConfiguration getCyclonConfiguration() {
		return this.croupierConfiguration;
	}

	public SearchConfiguration getApplicationConfiguration() {
		return this.applicationConfiguration;
	}

	public TManConfiguration getTManConfiguration() {
		return this.tManConfiguration;
	}
	
	public ElectionConfiguration getElectionConfiguration() {
		return this.electionConfiguration;
	}
}