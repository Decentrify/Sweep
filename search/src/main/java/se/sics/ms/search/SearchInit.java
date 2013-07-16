package se.sics.ms.search;

import se.sics.gvod.common.Self;
import se.sics.kompics.Init;
import se.sics.ms.configuration.SearchConfiguration;


/**
 * The init event for Search.
 */
public final class SearchInit extends Init {
	private final Self peerSelf;
	private final SearchConfiguration configuration;

	/**
	 * @param peerSelf
	 *            the address of the local node
	 * @param configuration
	 *            the {@link SearchConfiguration} of the local node
	 */
	public SearchInit(Self peerSelf, SearchConfiguration configuration) {
		super();
		this.peerSelf = peerSelf;
		this.configuration = configuration;
	}

	/**
	 * @return the address of the local node
	 */
	public Self getSelf() {
		return this.peerSelf;
	}

	/**
	 * @return the {@link SearchConfiguration} of the local node
	 */
	public SearchConfiguration getConfiguration() {
		return this.configuration;
	}
}