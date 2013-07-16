package se.sics.ms.peer;

import se.sics.gvod.common.Self;
import se.sics.gvod.config.CroupierConfiguration;
import se.sics.gvod.config.ElectionConfiguration;
import se.sics.gvod.config.GradientConfiguration;
import se.sics.gvod.config.SearchConfiguration;
import se.sics.kompics.Init;

public final class SearchPeerInit extends Init {
    private final Self self;
	private final CroupierConfiguration croupierConfiguration;
	private final SearchConfiguration applicationConfiguration;
	private final GradientConfiguration tManConfiguration;
	private final ElectionConfiguration electionConfiguration;

	public SearchPeerInit(Self self, 
                          CroupierConfiguration croupierConfiguration, SearchConfiguration applicationConfiguration,
			GradientConfiguration tManConfiguration, ElectionConfiguration electionConfiguration) {
		super();
		this.self = self;
		this.croupierConfiguration = croupierConfiguration;
		this.applicationConfiguration = applicationConfiguration;
		this.tManConfiguration = tManConfiguration;
		this.electionConfiguration = electionConfiguration;
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
		return this.tManConfiguration;
	}
	
	public ElectionConfiguration getElectionConfiguration() {
		return this.electionConfiguration;
	}
}