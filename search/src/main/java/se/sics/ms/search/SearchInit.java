package se.sics.ms.search;

import se.sics.gvod.config.SearchConfiguration;
import se.sics.kompics.Init;
import se.sics.ms.common.ApplicationSelf;

import java.security.PrivateKey;
import java.security.PublicKey;


/**
 * The init event for Search.
 */
public final class SearchInit extends Init<NPAwareSearch> {
    
	private final ApplicationSelf self;
	private final SearchConfiguration configuration;
    private PublicKey publicKey;
    private PrivateKey privateKey;
    private long seed;
    
	/**
	 * @param self
	 *            the address of the local node
	 * @param configuration
	 *            the {@link SearchConfiguration} of the local node
	 */
	public SearchInit(long seed, ApplicationSelf self, SearchConfiguration configuration, PublicKey publicKey, PrivateKey privateKey) {
		super();
        this.seed = seed;
		this.self = self;
		this.configuration = configuration;
        this.publicKey = publicKey;
        this.privateKey = privateKey;
	}


    /**
     * Get the seed for the system.
     * @return seed
     */
    public long getSeed(){
        return this.seed;
    }

	/**
	 * @return the address of the local node
	 */
	public ApplicationSelf getSelf() {
		return this.self;
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