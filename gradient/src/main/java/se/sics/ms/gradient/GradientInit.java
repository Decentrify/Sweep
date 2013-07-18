package se.sics.ms.gradient;

import se.sics.gvod.common.Self;
import se.sics.gvod.config.GradientConfiguration;
import se.sics.kompics.Init;

/**
 * The init event for the gradient.
 */
public final class GradientInit extends Init {
	private final Self self;
	private final GradientConfiguration configuration;

	/**
	 * @param self
	 *            the address of the local node
	 * @param configuration
	 *            the configuration file
	 */
	public GradientInit(Self self, GradientConfiguration configuration) {
		super();
		this.self = self;
		this.configuration = configuration;
	}

	/**
	 * @return the address of the local node
	 */
	public Self getSelf() {
		return this.self;
	}

	/**
	 * @return the configuration file
	 */
	public GradientConfiguration getConfiguration() {
		return this.configuration;
	}
}