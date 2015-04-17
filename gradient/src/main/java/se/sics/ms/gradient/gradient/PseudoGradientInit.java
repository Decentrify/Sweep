package se.sics.ms.gradient.gradient;

import se.sics.gvod.common.Self;
import se.sics.gvod.config.GradientConfiguration;
import se.sics.kompics.Init;
import se.sics.ms.common.ApplicationSelf;

/**
 * Initialization to the pseudo gradient component in application.
 * Created by babbarshaer on 2015-03-05.
 */
public class PseudoGradientInit extends Init<PseudoGradient>{

    private final ApplicationSelf self;
    private final GradientConfiguration configuration;

    /**
     * @param self
     *            the address of the local node
     * @param configuration
     *            the configuration file
     */
    public PseudoGradientInit(ApplicationSelf self, GradientConfiguration configuration) {
        super();
        this.self = self;
        this.configuration = configuration;
    }

    /**
     * @return the address of the local node
     */
    public ApplicationSelf getSelf() {
        return this.self;
    }

    /**
     * @return the configuration file
     */
    public GradientConfiguration getConfiguration() {
        return this.configuration;
    }
    
}
