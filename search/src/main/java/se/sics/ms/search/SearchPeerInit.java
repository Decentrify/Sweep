package se.sics.ms.search;

import se.sics.gvod.common.Self;
import se.sics.gvod.config.CroupierConfiguration;
import se.sics.gvod.config.ElectionConfiguration;
import se.sics.gvod.config.GradientConfiguration;
import se.sics.gvod.config.SearchConfiguration;
import se.sics.gvod.net.VodAddress;
import se.sics.kompics.Init;

public final class SearchPeerInit extends Init {
    private final Self self;
	private final CroupierConfiguration croupierConfiguration;
	private final SearchConfiguration applicationConfiguration;
	private final GradientConfiguration gradientConfiguration;
	private final ElectionConfiguration electionConfiguration;
    private final VodAddress bootstrappingNode;

	public SearchPeerInit(Self self,
                          CroupierConfiguration croupierConfiguration, SearchConfiguration applicationConfiguration,
                          GradientConfiguration gradientConfiguration, ElectionConfiguration electionConfiguration,
                          VodAddress bootstrappingNode) {
		super();
		this.self = self;
		this.croupierConfiguration = croupierConfiguration;
		this.applicationConfiguration = applicationConfiguration;
		this.gradientConfiguration = gradientConfiguration;
		this.electionConfiguration = electionConfiguration;
        this.bootstrappingNode = bootstrappingNode;
    }

	public Self getSelf() {
		return this.self;
	}

	public CroupierConfiguration getCroupierConfiguration() {
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
}