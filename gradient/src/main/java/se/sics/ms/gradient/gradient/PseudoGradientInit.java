package se.sics.ms.gradient.gradient;

import se.sics.gvod.common.Self;
import se.sics.gvod.config.GradientConfiguration;
import se.sics.kompics.Init;

/**
 * Created by babbarshaer on 2015-03-05.
 */
public class PseudoGradientInit extends Init<PseudoGradient>{

    private final Self self;
    private final GradientConfiguration configuration;

    /**
     * @param self
     *            the address of the local node
     * @param configuration
     *            the configuration file
     */
    public PseudoGradientInit(Self self, GradientConfiguration configuration) {
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
