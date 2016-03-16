/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.ms.search;

import se.sics.ktoolbox.util.config.KConfigOption;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class SearchPeerKConfig {
    public final static KConfigOption.Basic<Integer> sweepOverlayPrefix = new KConfigOption.Basic("overlayOwners.sweep", Integer.class);
}
