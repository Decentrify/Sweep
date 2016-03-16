/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.ms.search;

import se.sics.kompics.config.Config;
import se.sics.ktoolbox.util.config.KConfigHelper;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.overlays.id.OverlayIdHelper;
import se.sics.ktoolbox.util.overlays.id.OverlayIdRegistry;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class SearchPeerKCWrapper {
    private final Config configCore;
    private final byte sweepOverlayPrefix;
    public final Identifier tgradientId;
    
    public SearchPeerKCWrapper(Config configCore) {
        this.configCore = configCore;
        int intPrefix = KConfigHelper.read(configCore, SearchPeerKConfig.sweepOverlayPrefix);
        if(intPrefix > 15) {
            throw new RuntimeException("Only allow 16 owners 0 - 4bits currently");
        }
        byte ownerPrefix = (byte)intPrefix;
        sweepOverlayPrefix = (byte)(ownerPrefix << 4);
        OverlayIdRegistry.registerPrefix("Sweep", sweepOverlayPrefix);
        tgradientId = OverlayIdHelper.getIntIdentifier(sweepOverlayPrefix, OverlayIdHelper.Type.TGRADIENT, new byte[]{0,0,1});
    }
}
