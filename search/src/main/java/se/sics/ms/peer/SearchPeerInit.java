package se.sics.ms.peer;

import se.sics.gvod.common.Self;
import se.sics.gvod.config.CroupierConfiguration;
import se.sics.kompics.Init;
import se.sics.kompics.p2p.bootstrap.BootstrapConfiguration;

import se.sics.ms.configuration.ElectionConfiguration;
import se.sics.ms.configuration.GradientConfiguration;
import se.sics.ms.configuration.SearchConfiguration;

public final class SearchPeerInit extends Init {
    private final Self self;
	private final BootstrapConfiguration bootstrapConfiguration;
	private final CroupierConfiguration croupierConfiguration;
	private final SearchConfiguration applicationConfiguration;
	private final GradientConfiguration tManConfiguration;
	private final ElectionConfiguration electionConfiguration;

	public SearchPeerInit(Self self, BootstrapConfiguration bootstrapConfiguration,
                          CroupierConfiguration croupierConfiguration, SearchConfiguration applicationConfiguration,
			GradientConfiguration tManConfiguration, ElectionConfiguration electionConfiguration) {
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

	public CroupierConfiguration getCroupierConfiguration() {
		return this.croupierConfiguration;
	}

	public SearchConfiguration getApplicationConfiguration() {
		return this.applicationConfiguration;
	}

	public GradientConfiguration getGradientConfiguration() {
		return this.tManConfiguration;
	}
	
	public ElectionConfiguration getElectionConfiguration() {
		return this.electionConfiguration;
	}
}