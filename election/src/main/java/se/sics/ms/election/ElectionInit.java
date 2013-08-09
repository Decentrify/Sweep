package se.sics.ms.election;

import se.sics.gvod.common.Self;
import se.sics.gvod.config.ElectionConfiguration;
import se.sics.kompics.Init;

/**
 * QueryLimit class that carries the configuration variables for election as well as it's
 * own address from the boot strapping process to the election components
 */
public class ElectionInit extends Init {
	private final Self self;
	private final ElectionConfiguration config;

	/**
	 * Default constructor
	 * 
	 * @param self
	 *            The node's own address
	 * @param config
	 *            QueryLimit reference to the configuration file containing the
	 *            configuration values
	 */
	public ElectionInit(Self self, ElectionConfiguration config) {
		super();
		this.self = self;
		this.config = config;
	}

	/**
	 * Getter for the node's own address
	 * 
	 * @return the node's address
	 */
	public Self getSelf() {
		return this.self;
	}

	/**
	 * Getter for the election configuration file
	 * 
	 * @return a eference to the configuration file containing the configuration
	 *         values
	 */
	public ElectionConfiguration getConfig() {
		return this.config;
	}
}
