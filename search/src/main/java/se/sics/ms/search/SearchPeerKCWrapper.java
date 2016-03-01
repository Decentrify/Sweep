/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.ms.search;

import com.google.common.primitives.Ints;
import org.javatuples.Triplet;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.IntIdentifier;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class SearchPeerKCWrapper {
    public static final byte SP_PREFIX = (byte)-1;
    public final Identifier croupierId;
    public final Identifier gradientId;
    public final Identifier tgradientId;
    
    public SearchPeerKCWrapper() {
        croupierId = new IntIdentifier(Ints.fromBytes(SP_PREFIX, (byte)0, (byte)0, (byte)1));
        gradientId = new IntIdentifier(Ints.fromBytes(SP_PREFIX, (byte)0, (byte)0, (byte)2));
        tgradientId = new IntIdentifier(Ints.fromBytes(SP_PREFIX, (byte)0, (byte)0, (byte)3));
    }
}
