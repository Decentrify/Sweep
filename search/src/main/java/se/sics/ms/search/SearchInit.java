package se.sics.ms.search;

import se.sics.gvod.common.Self;
import se.sics.gvod.config.SearchConfiguration;
import se.sics.kompics.Init;

import java.security.PrivateKey;
import java.security.PublicKey;


/**
 * The init event for Search.
 */
public final class SearchInit extends Init<Search> {
    
	private final Self peerSelf;
	private final SearchConfiguration configuration;
    private PublicKey publicKey;
    private PrivateKey privateKey;
    
	/**
	 * @param peerSelf
	 *            the address of the local node
	 * @param configuration
	 *            the {@link SearchConfiguration} of the local node
	 */
	public SearchInit(Self peerSelf, SearchConfiguration configuration, PublicKey publicKey, PrivateKey privateKey) {
		super();
		this.peerSelf = peerSelf;
		this.configuration = configuration;
        this.publicKey = publicKey;
        this.privateKey = privateKey;
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

    
    public PublicKey getPublicKey() {
        return publicKey;
    }

    public PrivateKey getPrivateKey() {
        return privateKey;
    }
}