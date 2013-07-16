package election.system.peer.election;

import common.configuration.ElectionConfiguration;

import se.sics.gvod.common.Self;
import se.sics.kompics.Init;
import se.sics.kompics.address.Address;

/**
 * A class that carries the configuration variables for election as well as it's
 * own address from the boot strapping process to the election components
 */
public class ElectionInit extends Init {
	private final Self self;
	private final ElectionConfiguration electionConfiguration;

	/**
	 * Default constructor
	 * 
	 * @param self
	 *            The node's own address
	 * @param electionConfiguration
	 *            A reference to the configuration file containing the
	 *            configuration values
	 */
	public ElectionInit(Self self, ElectionConfiguration electionConfiguration) {
		super();
		this.self = self;
		this.electionConfiguration = electionConfiguration;
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
	public ElectionConfiguration getElectionConfiguration() {
		return this.electionConfiguration;
	}
}
