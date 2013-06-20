package cyclon;

import se.sics.kompics.Init;
import se.sics.kompics.address.Address;

import common.configuration.CyclonConfiguration;

public final class CyclonInit extends Init {
	private final CyclonConfiguration configuration;
	private final Address self;

	public CyclonInit(Address self, CyclonConfiguration configuration) {
		super();
		this.self = self;
		this.configuration = configuration;
	}

	public CyclonConfiguration getConfiguration() {
		return configuration;
	}

	public Address getSelf() {
		return self;
	}
}