package search.system.peer;

import se.sics.kompics.Init;
import se.sics.kompics.address.Address;
import se.sics.kompics.p2p.bootstrap.BootstrapConfiguration;

import common.configuration.CyclonConfiguration;
import common.configuration.ElectionConfiguration;
import common.configuration.SearchConfiguration;
import common.configuration.TManConfiguration;

public final class SearchPeerInit extends Init {
	private final Address peerSelf;
	private final BootstrapConfiguration bootstrapConfiguration;
	private final CyclonConfiguration cyclonConfiguration;
	private final SearchConfiguration applicationConfiguration;
	private final TManConfiguration tManConfiguration;
	private final ElectionConfiguration electionConfiguration;

	public SearchPeerInit(Address peerSelf, BootstrapConfiguration bootstrapConfiguration,
			CyclonConfiguration cyclonConfiguration, SearchConfiguration applicationConfiguration,
			TManConfiguration tManConfiguration, ElectionConfiguration electionConfiguration) {
		super();
		this.peerSelf = peerSelf;
		this.bootstrapConfiguration = bootstrapConfiguration;
		this.cyclonConfiguration = cyclonConfiguration;
		this.applicationConfiguration = applicationConfiguration;
		this.tManConfiguration = tManConfiguration;
		this.electionConfiguration = electionConfiguration;
	}

	public Address getPeerSelf() {
		return this.peerSelf;
	}

	public BootstrapConfiguration getBootstrapConfiguration() {
		return this.bootstrapConfiguration;
	}

	public CyclonConfiguration getCyclonConfiguration() {
		return this.cyclonConfiguration;
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