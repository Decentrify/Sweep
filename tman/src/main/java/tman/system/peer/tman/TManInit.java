package tman.system.peer.tman;

import common.configuration.TManConfiguration;
import se.sics.kompics.Init;
import se.sics.kompics.address.Address;

/**
 * The init event for TMan.
 */
public final class TManInit extends Init {
	private final Address self;
	private final TManConfiguration configuration;

	/**
	 * @param self
	 *            the address of the local node
	 * @param configuration
	 *            the configuration file
	 */
	public TManInit(Address self, TManConfiguration configuration) {
		super();
		this.self = self;
		this.configuration = configuration;
	}

	/**
	 * @return the address of the local node
	 */
	public Address getSelf() {
		return this.self;
	}

	/**
	 * @return the configuration file
	 */
	public TManConfiguration getConfiguration() {
		return this.configuration;
	}
}