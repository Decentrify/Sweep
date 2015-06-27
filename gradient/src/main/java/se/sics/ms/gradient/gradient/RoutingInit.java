package se.sics.ms.gradient.gradient;

import se.sics.gvod.config.GradientConfiguration;
import se.sics.kompics.Init;
import se.sics.ms.common.ApplicationSelf;

/**
 * Initialization to the pseudo gradient component in application.
 * Created by babbarshaer on 2015-03-05.
 */
public class RoutingInit extends Init<Routing>{

    private final long seed;
    private final ApplicationSelf self;
    private final GradientConfiguration configuration;

    /**
     * @param self
     *            the address of the local node
     * @param configuration
     *            the configuration file
     */
    public RoutingInit(long seed, ApplicationSelf self, GradientConfiguration configuration) {
        super();
        this.seed = seed;
        this.self = self;
        this.configuration = configuration;
    }

    /**
     * Seed from the application to be used inside the
     * component.
     *
     * @return seed.
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
     * @return the configuration file
     */
    public GradientConfiguration getConfiguration() {
        return this.configuration;
    }
    
}
