package se.sics.ms.gradient.gradient;

import se.sics.kompics.Init;
import se.sics.kompics.config.Config;
import se.sics.ktoolbox.gradient.GradientKCWrapper;
import se.sics.ktoolbox.util.config.impl.SystemKCWrapper;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * Initializer for the Partition Aware Gradient Component.
 *  
 * Created by babbarshaer on 2015-06-03.
 */
public class PAGInit extends Init<PartitionAwareGradient> {
    
    public final Config config;
    public final KAddress selfAdr;
    public final Identifier overlayId;
    public final int historyBufferSize;

    public PAGInit(Config config, KAddress selfAdr, Identifier overlayId, int historyBufferSize){
        this.config = config;
        this.selfAdr = selfAdr;
        this.overlayId = overlayId;
        this.historyBufferSize = historyBufferSize;
    }
}
