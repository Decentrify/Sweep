package se.sics.ms.gradient.gradient;

import se.sics.kompics.Init;
import se.sics.p2ptoolbox.gradient.GradientConfig;
import se.sics.p2ptoolbox.util.config.SystemConfig;
import se.sics.p2ptoolbox.util.network.impl.BasicAddress;

/**
 * Initializer for the Partition Aware Gradient Component.
 *  
 * Created by babbarshaer on 2015-06-03.
 */
public class PAGInit extends Init<PartitionAwareGradient> {
    
    
    private SystemConfig systemConfig;
    private GradientConfig gradientConfig;
    private BasicAddress basicAddress;
    private int overlayId;

    public PAGInit(SystemConfig systemConfig, GradientConfig gradientConfig, BasicAddress basicAddress, int overlayId){
        
        this.systemConfig = systemConfig;
        this.gradientConfig = gradientConfig;
        this.basicAddress = basicAddress;
        this.overlayId = overlayId;
    }


    public SystemConfig getSystemConfig() {
        return systemConfig;
    }

    public GradientConfig getGradientConfig() {
        return gradientConfig;
    }

    public BasicAddress getBasicAddress() { return basicAddress; }

    public int getOverlayId() {
        return overlayId;
    }
}
