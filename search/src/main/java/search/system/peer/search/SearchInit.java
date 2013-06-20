package search.system.peer.search;

import se.sics.kompics.Init;
import se.sics.kompics.address.Address;

import common.configuration.SearchConfiguration;

/**
 * The init event for Search.
 */
public final class SearchInit extends Init {
	private final Address peerSelf;
	private final SearchConfiguration configuration;

	/**
	 * @param peerSelf
	 *            the address of the local node
	 * @param configuration
	 *            the {@link SearchConfiguration} of the local node
	 */
	public SearchInit(Address peerSelf, SearchConfiguration configuration) {
		super();
		this.peerSelf = peerSelf;
		this.configuration = configuration;
	}

	/**
	 * @return the address of the local node
	 */
	public Address getSelf() {
		return this.peerSelf;
	}

	/**
	 * @return the {@link SearchConfiguration} of the local node
	 */
	public SearchConfiguration getConfiguration() {
		return this.configuration;
	}
}