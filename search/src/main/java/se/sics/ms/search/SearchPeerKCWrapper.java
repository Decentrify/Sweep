/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.ms.search;

import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifier.OverlayIdHelper;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class SearchPeerKCWrapper {
    public static final byte SP_PREFIX = (byte)-1;
    public final Identifier tgradientId;
    
    public SearchPeerKCWrapper() {
        tgradientId = OverlayIdHelper.getIntIdentifier(SP_PREFIX, OverlayIdHelper.Type.TGRADIENT, new byte[]{0,0,1});
    }
}
